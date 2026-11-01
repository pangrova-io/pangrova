package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGCompletionEvaluator;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine.StageDispatchRecord;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskTerminalRegistry;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns per-query lifecycle state, cancellation, terminal routing, and lineage events. */
@org.pangrova.common.concurrent.ThreadSafe
public final class DAGLifecycleController {
    private static final Logger LOG =
            LoggerFactory.getLogger(DAGLifecycleController.class);

    private final TaskDispatcher dispatcher;
    private final DispatchCanceller canceller;
    private final ShuffleLifecycleManager shuffleLifecycleManager;
    private final LineageEventEmitter lineageEmitter;
    private final List<StageLifecycleHandler> handlers;
    private final TaskTerminalRegistry terminalRegistry = new TaskTerminalRegistry();
    private final ConcurrentHashMap<String, DAGQueryContext> queryContexts =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StageDAG> queryDags =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> terminalSignals =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DAGExecutionState> liveExecutions =
            new ConcurrentHashMap<>();

    /** Creates the lifecycle owner for one shared dispatch engine. */
    public DAGLifecycleController(
            TaskDispatcher dispatcher,
            DispatchCanceller canceller,
            ShuffleLifecycleManager shuffleLifecycleManager,
            LineageEventEmitter lineageEmitter,
            List<StageLifecycleHandler> handlers) {
        this.dispatcher = dispatcher;
        this.canceller = canceller;
        this.shuffleLifecycleManager = shuffleLifecycleManager;
        this.lineageEmitter = lineageEmitter;
        this.handlers = List.copyOf(handlers);
    }

    /** Registers the DAG and optional immutable context before dispatch. */
    public void registerQuery(
            String queryId, StageDAG dag, DAGQueryContext context) {
        terminalSignals.putIfAbsent(queryId, new CompletableFuture<>());
        queryDags.put(queryId, dag);
        if (context != null) {
            queryContexts.put(queryId, context);
        }
    }

    /** Registers the DAG only when it has not already been registered. */
    public void registerDagIfAbsent(String queryId, StageDAG dag) {
        terminalSignals.putIfAbsent(queryId, new CompletableFuture<>());
        queryDags.putIfAbsent(queryId, dag);
    }

    /** Registers a terminal observer without polling the dispatch state. */
    public void onQueryTerminal(String queryId, Consumer<Boolean> listener) {
        CompletableFuture<Boolean> signal = terminalSignals.get(queryId);
        if (signal == null) {
            listener.accept(false);
            return;
        }
        signal.thenAccept(listener);
    }

    /** Owns a live dispatch wait before scheduling; durable passes have their own control owner. */
    public void registerLiveExecution(DAGExecutionState state) {
        if (liveExecutions.putIfAbsent(state.queryId(), state) != null) {
            throw new IllegalStateException("Query dispatch is already active: " + state.queryId());
        }
        state.completion().whenComplete((result, failure) ->
                liveExecutions.remove(state.queryId(), state));
        // A terminal cleanup may race registration before the first task is scheduled.
        if (queryDags.get(state.queryId()) != state.dag()) {
            cancelExecution(state);
        }
    }

    /** Returns the registered immutable query context. */
    public DAGQueryContext queryContext(String queryId) {
        return queryContexts.get(queryId);
    }

    /** Registers a task in the terminal-state correlator. */
    public void registerTask(String queryId, String taskId, Stage stage) {
        terminalRegistry.register(queryId, taskId, stage);
    }

    /** Routes a terminal task report to its owning stage. */
    public void onTaskTerminal(
            String queryId, String taskId, String state, ErrorDetail error) {
        terminalRegistry.onTerminal(queryId, taskId, state, error);
    }

    /** Returns whether every tracked stage has finished. */
    public boolean isQueryComplete(String queryId) {
        StageDAG dag = queryDags.get(queryId);
        return dag == null || DAGCompletionEvaluator.allStagesFinished(dag);
    }

    /** Invokes a stage lifecycle callback while isolating handler failures. */
    public void fireStageEvent(
            Stage stage,
            StageDAG dag,
            String queryId,
            org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext execution,
            BiConsumer<StageLifecycleHandler, StageLifecycleEvent> call) {
        StageLifecycleEvent event = new StageLifecycleEvent(
                stage, dag, queryId, execution);
        for (StageLifecycleHandler handler : handlers) {
            if (!(execution instanceof StageExecutionContext.Live)
                    && !(handler instanceof ShuffleStageHandler)) {
                continue;
            }
            if (handler instanceof ShuffleStageHandler) {
                call.accept(handler, event);
            } else {
                invokeHandler(handler, call, event, stage, queryId);
            }
            if (stage.stateMachine().getState() == StageStateMachine.State.PLANNED) {
                break;
            }
        }
    }

    /** Completes query lifecycle state and publishes the terminal dispatch result. */
    public void completeQuery(
            String queryId,
            ConcurrentHashMap<Integer, DispatchResult> results,
            StageDAG dag,
            CompletableFuture<DispatchResult> completion,
            List<StageDispatchRecord> dispatched) {
        DAGCompletionEvaluator.completeWithLastResult(
                results, dag, completion, dispatched);
        fireQueryCompleted(queryId);
    }

    /** Cancels downstream stages and completes the query exceptionally. */
    public void failStage(
            Stage failed,
            StageDAG dag,
            List<StageDispatchRecord> dispatched,
            CompletableFuture<DispatchResult> completion,
            String queryId,
            Throwable dispatchFailure) {
        cancelDownstreamStages(failed, dag, dispatched);
        fireQueryFailed(queryId);
        if (dispatchFailure != null) {
            completion.completeExceptionally(dispatchFailure);
            return;
        }
        ErrorDetail error = failed.lastError();
        String message = error == null
                ? "Stage " + failed.stageId() + " failed" : error.message();
        completion.completeExceptionally(new PangrovaException(
                PangrovaErrorCode.RUNTIME_TASK_FAILED, message));
    }

    /** Cancels all dispatched tasks and clears the failed query. */
    public void failDispatch(
            String queryId,
            AtomicBoolean cancelled,
            List<StageDispatchRecord> dispatched) {
        cancelled.set(true);
        canceller.cancelAllTasks(queryId, dispatched);
        fireQueryFailed(queryId);
    }

    /** Clears a failed query and notifies lifecycle handlers. */
    public void failQuery(String queryId) {
        fireQueryFailed(queryId);
    }

    /** Emits a stage start or completion lineage event when governance is enabled. */
    public void emitStageLineage(String queryId, StageNode stage, boolean start) {
        if (lineageEmitter == null) {
            LOG.debug("event=lineage_skip reason=governance_disabled queryId={}"
                    + " stageId={}", queryId, stage.stageId());
            return;
        }
        String runId = queryId + "/stage-" + stage.stageId();
        if (start) {
            lineageEmitter.emitStartEvent(runId, null, List.of(), List.of());
        } else {
            lineageEmitter.emitCompleteEvent(runId);
        }
    }

    /** Clears all state retained for a query. */
    public void clearQuery(String queryId) {
        queryDags.remove(queryId);
        DAGExecutionState state = liveExecutions.remove(queryId);
        if (state != null) {
            cancelExecution(state);
        }
        queryContexts.remove(queryId);
        terminalRegistry.clear(queryId);
        CompletableFuture<Boolean> signal = terminalSignals.remove(queryId);
        if (signal != null) {
            signal.complete(false);
        }
    }

    private static void cancelExecution(DAGExecutionState state) {
        state.cancelled().set(true);
        state.completion().completeExceptionally(new CancellationException(
                "Query dispatch was cleared: " + state.queryId()));
    }

    private static void invokeHandler(
            StageLifecycleHandler handler,
            BiConsumer<StageLifecycleHandler, StageLifecycleEvent> call,
            StageLifecycleEvent event,
            Stage stage,
            String queryId) {
        try {
            call.accept(handler, event);
        } catch (Exception failure) {
            LOG.warn("event=handler_exception handler={} queryId={} stageId={} error={}",
                    handler.getClass().getSimpleName(), queryId,
                    stage.stageId(), failure.getMessage(), failure);
        }
    }

    private void cancelDownstreamStages(
            Stage failed, StageDAG dag, List<StageDispatchRecord> dispatched) {
        Set<Integer> stageIds = collectTransitiveDownstream(failed.stageId(), dag);
        for (Stage stage : dag.stageEntities()) {
            if (stageIds.contains(stage.stageId()) && !isTerminal(stage)) {
                cancelStage(stage, dispatched);
            }
        }
    }

    private static Set<Integer> collectTransitiveDownstream(
            int rootStageId, StageDAG dag) {
        Set<Integer> result = new HashSet<>();
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        queue.add(rootStageId);
        while (!queue.isEmpty()) {
            for (int downstream : dag.directDownstream(queue.poll())) {
                if (result.add(downstream)) {
                    queue.add(downstream);
                }
            }
        }
        return result;
    }

    private void cancelStage(Stage stage, List<StageDispatchRecord> dispatched) {
        StageStateMachine.State state = stage.stateMachine().getState();
        stage.stateMachine().tryTransitionTo(state == StageStateMachine.State.PLANNED
                ? StageStateMachine.State.CANCELLED : StageStateMachine.State.FAILED);
        for (String taskId : stage.taskIds()) {
            StageDispatchRecord record =
                    DAGCompletionEvaluator.findDispatchRecord(taskId, dispatched);
            if (record != null) {
                canceller.cancelTask(stage.stateMachine().getQueryId(), taskId,
                        record.result().workerHost(), record.workerHttpPort());
            }
        }
    }

    private static boolean isTerminal(Stage stage) {
        StageStateMachine.State state = stage.stateMachine().getState();
        return state == StageStateMachine.State.FINISHED
                || state == StageStateMachine.State.FAILED
                || state == StageStateMachine.State.CANCELLED;
    }

    private void fireQueryCompleted(String queryId) {
        if (!claimTerminalNotification(queryId)) {
            return;
        }
        handlers.forEach(handler -> invokeQueryHandler(handler, queryId, true));
        completeTerminalSignal(queryId, true);
    }

    private void fireQueryFailed(String queryId) {
        var context = queryContexts.get(queryId);
        if (context != null && !(context.execution()
                instanceof StageExecutionContext.Live)) {
            // A failed durable control pass must not delete producer bytes needed by fenced recovery.
            clearQuery(queryId);
            return;
        }
        if (!claimTerminalNotification(queryId)) {
            return;
        }
        handlers.forEach(handler -> invokeQueryHandler(handler, queryId, false));
        completeTerminalSignal(queryId, false);
    }

    private void completeTerminalSignal(String queryId, boolean successful) {
        CompletableFuture<Boolean> signal = terminalSignals.get(queryId);
        if (signal != null) {
            signal.complete(successful);
        }
    }

    /** Completes a durable graph without manufacturing a client row/result endpoint. */
    public void completeDurable(String queryId, CompletableFuture<DispatchResult> completion) {
        fireQueryCompleted(queryId);
        completion.complete(null);
    }

    private boolean claimTerminalNotification(String queryId) {
        boolean registered = queryDags.remove(queryId) != null;
        queryContexts.remove(queryId);
        terminalRegistry.clear(queryId);
        return registered;
    }

    private static void invokeQueryHandler(
            StageLifecycleHandler handler, String queryId, boolean completed) {
        try {
            if (completed) {
                handler.onQueryCompleted(queryId);
            } else {
                handler.onQueryFailed(queryId);
            }
        } catch (Exception failure) {
            LOG.warn("event=handler_exception handler={} queryId={} error={}",
                    handler.getClass().getSimpleName(), queryId,
                    failure.getMessage(), failure);
        }
    }
}
