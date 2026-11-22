package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.query.api.enums.StageState;

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

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.task.StageDispatchRecord;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskTerminalRegistry;
import org.pangrova.coordinator.query.task.QueryStageObservations;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.pangrova.query.api.stage.StageCompletionCallback.SuccessfulAttempt;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.query.api.error.QueryFailure;
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

    private record RegisteredDAG(StageDAG dag, TaskTerminalRegistry terminals) {}

    private final ConcurrentHashMap<String, QueryExecutionScope> queryContexts =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RegisteredDAG> queryDags =
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
    public synchronized void registerQuery(
            String queryId, StageDAG dag, QueryExecutionScope context) {
        registerDagIfAbsent(queryId, dag);
        if (context != null) {
            queryContexts.put(queryId, context);
        }
    }

    /** Registers the DAG only when it has not already been registered. */
    public synchronized void registerDagIfAbsent(String queryId, StageDAG dag) {
        queryDags.compute(queryId, (id, existing) -> {
            if (existing == null) {
                terminalSignals.put(id, new CompletableFuture<>());
                return new RegisteredDAG(dag, new TaskTerminalRegistry(id));
            }
            if (existing.dag() != dag) throw new IllegalStateException("Query already owns a different DAG: " + id);
            return existing;
        });
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
        if (!owns(state.queryId(), state.dag())) {
            cancelExecution(state);
        }
    }

    /** Returns the registered immutable query context. */
    public QueryExecutionScope queryContext(String queryId) {
        return queryContexts.get(queryId);
    }

    /** Records selections made by this exact live DAG's task terminal registry. */
    public void observeTaskSelections(String queryId, StageDAG dag, QueryStageObservations observations) {
        RegisteredDAG current = queryDags.get(queryId);
        if (current != null && current.dag() == dag) current.terminals().observeTaskSelections(observations);
    }

    /** Registers a task in the terminal-state correlator. */
    public void registerTask(String queryId, String taskId, Stage stage) {
        RegisteredDAG registration = queryDags.get(queryId);
        if (registration != null && registration.dag().findStage(stage.stageId()) == stage) {
            registration.terminals().register(queryId, taskId, stage);
        }
    }

    /**
     * Reserves one stage task slot for a retry dispatched after worker loss.
     *
     * <p>Only exchange-free tasks can be moved independently. An exchange producer
     * needs a new published placement, and consumers need renewable input read authority.</p>
     *
     * @return true when the retry slot is registered and the replacement may be
     *         dispatched; false when the task is unknown, the budget is exhausted,
     *         or a downstream stage has already started
     */
    public boolean tryPrepareTaskRetry(String queryId, String failedTaskId,
            String retryTaskId, int maxRetries) {
        var registration = queryDags.get(queryId);
        StageDAG dag = registration == null ? null : registration.dag();
        if (dag == null) return false;
        Stage stage = stageOwning(dag, failedTaskId);
        if (stage == null) return false;
        // Replacement requests currently clone the old attempt. They cannot rebind input
        // tickets/grants or replace the assignment from which downstream routes are built.
        if (!dag.edges().getOrDefault(stage.stageId(), List.of()).isEmpty()
                || !dag.directDownstream(stage.stageId()).isEmpty()) return false;
        if (!stage.tryPrepareTaskRetry(failedTaskId, retryTaskId, maxRetries)) {
            return false;
        }
        try {
            registration.terminals().register(queryId, retryTaskId, stage);
            if (owns(queryId, dag)) return true;
        } catch (RuntimeException | Error failure) {
            stage.rollbackTaskRetry(failedTaskId, retryTaskId);
            throw failure;
        }
        stage.rollbackTaskRetry(failedTaskId, retryTaskId);
        return false;
    }

    /** Reverts an uncommitted retry reservation after its dispatch failed. */
    public void rollbackTaskRetry(String queryId, String failedTaskId, String retryTaskId) {
        var registration = queryDags.get(queryId);
        StageDAG dag = registration == null ? null : registration.dag();
        if (dag == null) return;
        Stage stage = stageOwning(dag, retryTaskId);
        if (stage != null && stage.rollbackTaskRetry(failedTaskId, retryTaskId)) {
            registration.terminals().replayRestoredTask(failedTaskId, stage);
        }
    }

    private static Stage stageOwning(StageDAG dag, String taskId) {
        for (Stage candidate : dag.stageEntities()) {
            if (candidate.taskIds().contains(taskId)) return candidate;
        }
        return null;
    }

    /** Routes a terminal task report to its owning stage. */
    public void onTaskTerminal(
            String queryId, String taskId, String state, QueryFailure error) {
        onTaskTerminal(queryId, taskId, state, error, null);
    }

    /** Relays the summary only to the live owner of the original logical task slot. */
    public void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error,
            org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange range) {
        RegisteredDAG owner = queryDags.get(queryId);
        if (owner != null) owner.terminals().onTerminal(queryId, taskId, state, error, range);
    }

    /** Routes the selected physical attempt to its still-current logical slot. */
    public void onSuccessfulTaskTerminal(String queryId, String logicalTaskId, SuccessfulAttempt attempt,
            BuildKeyRange range) {
        var owner = queryDags.get(queryId);
        if (owner != null) owner.terminals().onSuccessfulTerminal(queryId, logicalTaskId, attempt, range);
    }

    /** A retained terminal signal also owns late reports until query cleanup. */
    public boolean ownsQueryCompletion(String queryId) {
        return terminalSignals.containsKey(queryId);
    }

    /** Returns whether every tracked stage has finished. */
    public boolean isQueryComplete(String queryId) {
        var registration = queryDags.get(queryId);
        StageDAG dag = registration == null ? null : registration.dag();
        return dag == null || DAGCompletionEvaluator.allStagesFinished(dag);
    }

    /** Invokes a stage lifecycle callback while isolating handler failures. */
    public void fireStageEvent(
            Stage stage,
            StageDAG dag,
            String queryId,
            org.pangrova.execution.authority.StageExecutionContext execution,
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
            if (stage.stateMachine().getState() == StageState.PLANNED) {
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
        finishQuery(queryId, dag, true);
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
        finishQuery(queryId, dag, false);
        if (dispatchFailure != null) {
            completion.completeExceptionally(dispatchFailure);
            return;
        }
        QueryFailure error = failed.lastError();
        String message = error == null
                ? "Stage " + failed.stageId() + " failed" : error.message();
        completion.completeExceptionally(new PangrovaException(
                PangrovaErrorCode.RUNTIME_TASK_FAILED, message));
    }

    /** Cancels all dispatched tasks and clears the failed query. */
    public void failDispatch(
            String queryId, StageDAG dag,
            AtomicBoolean cancelled,
            List<StageDispatchRecord> dispatched) {
        cancelled.set(true);
        canceller.cancelAllTasks(queryId, dispatched);
        finishQuery(queryId, dag, false);
    }

    /** Clears a failed query and notifies lifecycle handlers. */
    public void failQuery(String queryId, StageDAG dag) {
        finishQuery(queryId, dag, false);
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

    /** Stops scheduling and pending DAG finalizers while physical task drain may still be outstanding. */
    public void cancelLiveDispatch(String queryId) {
        DAGExecutionState state;
        CompletableFuture<Boolean> signal;
        synchronized (this) {
            retireDag(queryId);
            state = liveExecutions.remove(queryId);
            signal = terminalSignals.get(queryId);
        }
        if (state != null) cancelExecution(state);
        if (signal != null) signal.complete(false);
    }

    /** Clears all state retained for a query. */
    public void clearQuery(String queryId) {
        DAGExecutionState state;
        CompletableFuture<Boolean> signal;
        synchronized (this) {
            retireDag(queryId);
            state = liveExecutions.remove(queryId);
            queryContexts.remove(queryId);
            signal = terminalSignals.remove(queryId);
        }
        if (state != null) cancelExecution(state);
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
        StageState state = stage.stateMachine().getState();
        stage.stateMachine().tryTransitionTo(state == StageState.PLANNED
                ? StageState.CANCELLED : StageState.FAILED);
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
        StageState state = stage.stateMachine().getState();
        return state == StageState.FINISHED
                || state == StageState.FAILED
                || state == StageState.CANCELLED;
    }

    private void finishQuery(String queryId, StageDAG dag, boolean success) {
        CompletableFuture<Boolean> signal;
        boolean notifyHandlers;
        synchronized (this) {
            if (!owns(queryId, dag)) return;
            retireDag(queryId);
            var context = queryContexts.remove(queryId);
            // Fenced durable recovery retains producer bytes after a failed control pass.
            notifyHandlers = success || context == null || context.execution() instanceof StageExecutionContext.Live;
            signal = notifyHandlers ? terminalSignals.get(queryId) : terminalSignals.remove(queryId);
        }
        if (notifyHandlers) handlers.forEach(handler -> invokeQueryHandler(handler, queryId, success));
        if (signal != null) signal.complete(success);
    }

    /** Completes a durable graph without manufacturing a client row/result endpoint. */
    public void completeDurable(String queryId, StageDAG dag, CompletableFuture<DispatchResult> completion) {
        finishQuery(queryId, dag, true);
        completion.complete(null);
    }

    /** Checks whether a scheduling callback still belongs to the installed execution. */
    public boolean owns(String queryId, StageDAG dag) {
        var registration = queryDags.get(queryId);
        return registration != null && registration.dag() == dag;
    }

    private boolean retireDag(String queryId) {
        var registration = queryDags.remove(queryId);
        if (registration == null) return false;
        registration.terminals().close();
        return true;
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
