package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.generation.PublishedGenerationBindingResolver;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveStagePlanResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.DAGStagePlanBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGLifecycleController;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.execution.split.StageSplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.client.core.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives asynchronous event-based dispatch for multi-stage queries. */
@ThreadSafe
public final class DAGDispatchEngine implements StageCompletionCallback {
    private static final Logger LOG = LoggerFactory.getLogger(DAGDispatchEngine.class);

    private final PipelineConfig pipelineConfig;
    private final LeafStageDispatcher leafStageDispatcher;
    private final AdaptiveStagePlanResolver adaptivePlanResolver;
    private final DAGStagePlanBinder stagePlanBinder;
    private final StageSplitFetcher stageSplitFetcher;
    private final DAGLifecycleController lifecycle;
    private final org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DurableDAGControl durableControl;

    /** Creates a dispatch engine without adaptive sub-plan reoptimization. */
    public DAGDispatchEngine(
            DAGDispatchContext dependencies,
            List<StageLifecycleHandler> handlers,
            org.pangrova.coordinator.adapter.indexing.IndexAwareSplitFetcher splitFetcher) {
        this(dependencies, handlers, splitFetcher, null);
    }

    /** Creates a dispatch engine with the supplied execution dependencies. */
    public DAGDispatchEngine(
            DAGDispatchContext dependencies,
            List<StageLifecycleHandler> handlers,
            org.pangrova.coordinator.adapter.indexing.IndexAwareSplitFetcher splitFetcher,
            org.pangrova.planner.SubPlanReoptimizer subPlanReoptimizer) {
        DispatchCanceller canceller = new DispatchCanceller(
                dependencies.dispatcher(), dependencies.taskTracker());
        this.pipelineConfig = dependencies.pipelineConfig();
        var externalShuffle = dependencies.shuffleLifecycleManager()
                instanceof CoordinatorShuffleLifecycleManager manager ? manager : null;
        PublishedGenerationBindingResolver artifactResolver = splitFetcher == null
                ? null : splitFetcher.artifactBindingResolver();
        this.stagePlanBinder = new DAGStagePlanBinder(new RetrievalGenerationResolver(artifactResolver),
                splitFetcher == null ? null : splitFetcher.generatedValueReads());
        this.stageSplitFetcher = new StageSplitFetcher(dependencies.catalogRegistry(), splitFetcher);
        this.adaptivePlanResolver =
                new AdaptiveStagePlanResolver(subPlanReoptimizer);
        this.lifecycle = new DAGLifecycleController(
                dependencies.dispatcher(), canceller,
                dependencies.shuffleLifecycleManager(), dependencies.lineageEmitter(),
                handlers);
        this.leafStageDispatcher = new LeafStageDispatcher(
                dependencies.dispatcher(), dependencies.runtimeFilterRegistry(),
                dependencies.stateJournal(), dependencies.taskTimeoutSeconds(),
                canceller, lifecycle::registerTask,
                new LeafStageDispatcher.LeafDispatchOptions(
                        artifactResolver,
                        dependencies.shuffleLifecycleManager()
                                instanceof CoordinatorShuffleLifecycleManager,
                        dependencies.shuffleLifecycleManager()
                                instanceof CoordinatorShuffleLifecycleManager manager
                                        ? manager::bindTaskAccess
                                        : java.util.function.UnaryOperator.identity()));
        this.durableControl = new org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DurableDAGControl(
                lifecycle, externalShuffle);
    }

    /** Replays through this engine's same listener and ordinary root scheduling. */
    public CompletableFuture<DispatchResult> startDurable(DurableStageRuntime runtime) {
        return durableControl.start(runtime, state -> {
            registerListeners(state);
            scheduleRootStages(state);
        });
    }

    /** Dispatches a DAG with its immutable query context. */
    public DispatchResult dispatch(
            CoordinatorSessionContext session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            DAGQueryContext queryContext) {
        return dispatch(session, dag, compiler, workerSelector, timeoutMs,
                queryContext, ignored -> { });
    }

    /** Publishes the OUTPUT endpoint once Worker dispatch is acknowledged. */
    public DispatchResult dispatch(
            CoordinatorSessionContext session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            DAGQueryContext queryContext,
            Consumer<DispatchResult> resultReadySink) {
        lifecycle.registerQuery(session.queryId(), dag, queryContext);
        return dispatch(session, dag, compiler, workerSelector, timeoutMs,
                resultReadySink);
    }

    /** Dispatches a DAG using any previously registered query context. */
    public DispatchResult dispatch(
            CoordinatorSessionContext session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs) {
        return dispatch(session, dag, compiler, workerSelector, timeoutMs,
                ignored -> { });
    }

    private DispatchResult dispatch(
            CoordinatorSessionContext session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            Consumer<DispatchResult> resultReadySink) {
        lifecycle.registerDagIfAbsent(session.queryId(), dag);
        DAGExecutionState state = DAGExecutionState.create(new StageExecutionContext.Live(session), dag,
                compiler, workerSelector, null, resultReadySink);
        lifecycle.registerLiveExecution(state);
        try {
            registerListeners(state);
            scheduleRootStages(state);
            return DAGCompletionEvaluator.awaitCompletion(
                    state.completion(), timeoutMs, session.queryId());
        } catch (Exception failure) {
            lifecycle.failDispatch(
                    state.queryId(), state.cancelled(), state.dispatched());
            throw failure;
        }
    }

    private void registerListeners(DAGExecutionState state) {
        for (Stage stage : state.dag().stageEntities()) {
            stage.stateMachine().addStateChangeListener((id, previous, next) -> {
                updateRunningCount(previous, next, state.runningStageCount());
                try {
                    onStateChange(stage, next, state);
                } catch (Exception failure) {
                    LOG.error("event=stage_listener_exception stageId={} queryId={}"
                                    + " transition={}→{} error={}",
                            id, state.queryId(), previous, next,
                            failure.getMessage(), failure);
                    state.cancelled().set(true);
                    state.completion().completeExceptionally(failure);
                }
            });
        }
    }

    private void onStateChange(
            Stage stage, StageStateMachine.State next, DAGExecutionState state) {
        switch (next) {
            case RUNNING -> onStageRunning(stage, state);
            case FINISHED -> {
                fireHandlerEvent(stage, state, StageLifecycleHandler::onFinished);
                onStageFinished(state);
            }
            case FAILED -> onFailedState(stage, state);
            default -> {
            }
        }
    }

    private void onFailedState(Stage stage, DAGExecutionState state) {
        fireHandlerEvent(stage, state, StageLifecycleHandler::onFailed);
        if (stage.stateMachine().getState() == StageStateMachine.State.PLANNED) {
            state.dispatchFailures().remove(stage.stageId());
            LOG.info("event=stage_retry_reschedule queryId={} stageId={} retryCount={}",
                    state.queryId(), stage.stageId(), stage.retryCount());
            scheduleStage(stage, state);
            return;
        }
        state.cancelled().set(true);
        onStageFailed(stage, state, state.dispatchFailures().remove(stage.stageId()));
    }

    private static void updateRunningCount(
            StageStateMachine.State previous,
            StageStateMachine.State next,
            AtomicInteger runningCount) {
        if (next == StageStateMachine.State.RUNNING) {
            runningCount.incrementAndGet();
        } else if (previous == StageStateMachine.State.RUNNING
                && isRunningExit(next)) {
            runningCount.decrementAndGet();
        }
    }

    private static boolean isRunningExit(StageStateMachine.State state) {
        return state == StageStateMachine.State.FLUSHING
                || state == StageStateMachine.State.FINISHED
                || state == StageStateMachine.State.FAILED
                || state == StageStateMachine.State.CANCELLED;
    }

    private void fireHandlerEvent(
            Stage stage,
            DAGExecutionState state,
            java.util.function.BiConsumer<StageLifecycleHandler,
                    StageLifecycleEvent> call) {
        lifecycle.fireStageEvent(
                stage, state.dag(), state.queryId(),
                state.execution(),
                call);
    }

    private void scheduleRootStages(DAGExecutionState state) {
        List<Stage> roots = state.dag().rootStageEntities();
        if (roots.isEmpty()) {
            throw new IllegalStateException(
                    "DAG has no root stages for query " + state.queryId());
        }
        roots.forEach(stage -> scheduleStage(stage, state));
    }

    private void onStageRunning(Stage stage, DAGExecutionState state) {
        if (state.durable() != null || !pipelineConfig.enabled() || state.cancelled().get()) {
            return;
        }
        if (atConcurrencyLimit(state)) {
            LOG.debug("event=pipeline_backpressure queryId={} runningCount={}"
                            + " maxConcurrent={}",
                    state.queryId(), state.runningStageCount().get(),
                    pipelineConfig.maxConcurrentStages());
            return;
        }
        scheduleReadyDownstream(stage.stageId(), state);
    }

    private void onStageFinished(DAGExecutionState state) {
        if (DAGCompletionEvaluator.allStagesFinished(state.dag())) {
            completeQuery(state);
        } else if (!state.cancelled().get()) {
            scheduleReadyDownstream(-1, state);
        }
    }

    private void scheduleReadyDownstream(int triggerStageId, DAGExecutionState state) {
        Set<Integer> activeIds = activeStageIds(state);
        for (Stage downstream : state.dag().readyStageEntities(activeIds)) {
            if (state.cancelled().get() || atConcurrencyLimit(state)) {
                return;
            }
            if (downstream.stateMachine().getState()
                    == StageStateMachine.State.PLANNED) {
                logPipelineSchedule(triggerStageId, downstream, state.queryId());
                scheduleStage(downstream, state);
            }
        }
        if (!state.completion().isDone() && !state.cancelled().get()
                && DAGCompletionEvaluator.allStagesFinished(state.dag())) {
            completeQuery(state);
        }
    }

    private static Set<Integer> activeStageIds(DAGExecutionState state) {
        if (state.durable() == null) {
            return DAGCompletionEvaluator.collectActiveStageIds(state.dag());
        }
        return state.dag().stageEntities().stream()
                .filter(stage -> stage.stateMachine().getState() == StageStateMachine.State.FINISHED)
                .map(Stage::stageId).collect(java.util.stream.Collectors.toSet());
    }

    private boolean atConcurrencyLimit(DAGExecutionState state) {
        return pipelineConfig.maxConcurrentStages() > 0
                && state.runningStageCount().get()
                        >= pipelineConfig.maxConcurrentStages();
    }

    private static void logPipelineSchedule(
            int triggerStageId, Stage downstream, String queryId) {
        if (triggerStageId >= 0) {
            LOG.info("event=pipeline_schedule queryId={} triggerStageId={}"
                            + " downstreamStageId={}",
                    queryId, triggerStageId, downstream.stageId());
        }
    }

    private void scheduleStage(Stage stage, DAGExecutionState state) {
        if (state.cancelled().get()
                || !stage.stateMachine().tryTransitionTo(
                        StageStateMachine.State.SCHEDULING)) {
            return;
        }
        fireHandlerEvent(stage, state, StageLifecycleHandler::onScheduling);
        try {
            PreparedStage prepared = prepareStage(stage, state);
            List<PartitionDispatch> partitions =
                    leafStageDispatcher.dispatchLeafStage(prepared.dispatch());
            awaitStageDispatch(stage, prepared.plan(), partitions, state);
        } catch (Exception failure) {
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
            lifecycle.failQuery(state.queryId());
            throw propagateSchedulingFailure(failure);
        }
    }

    private PreparedStage prepareStage(Stage stage, DAGExecutionState state) {
        if (state.durable() != null) {
            return new PreparedStage(stage.plan(), new LeafStageDispatcher.LeafDispatch(
                    state.execution(), stage.plan(), state.durable().boundPlan(stage.plan()), List.of(), null,
                    state.dag(), state.dispatched(), null, false,
                    state.durable().control().planEpoch(stage.stageId()), state.durable()));
        }
        StageNode plan = adaptivePlanResolver.apply(stage.plan(), state.dag(), state.compiler());
        if (stage.retryCount() > 0) {
            state.dispatched().removeIf(record ->
                    record.stageId() == stage.stageId());
        }
        DAGQueryContext queryContext = lifecycle.queryContext(state.queryId());
        lifecycle.emitStageLineage(state.queryId(), plan, true);
        SerializedPlanFragment serialized = state.serializePlan(plan, queryContext);
        if (queryContext != null && !queryContext.retrievalArtifacts().isEmpty()) {
            LOG.info("event=retrieval_stage_hints queryId={} stageId={} hints={}",
                    state.queryId(), plan.stageId(), serialized.retrievalTopKHints().stream()
                            .map(hint -> hint.kind() + ":" + hint.sourceKey()).toList());
        }
        List<SplitFetcher.SplitWithPaths> splits = stageSplitFetcher.fetch(
                plan, state.dag(), state.queryId(), queryContext);
        BoundExecutionPlan boundPlan = stagePlanBinder.bind(new DAGStagePlanBinder.Input(
                serialized, plan, splits, queryContext, state.session(), state.workerSelector(),
                state.dag().findDownstreamEdge(plan.stageId()) == null));
        // Only the terminal stage owns the table write. Upstream stages must
        // produce their ExchangeRel shuffle output with a shuffle task binding.
        var writeConfig = queryContext == null
                || state.dag().findDownstreamEdge(plan.stageId()) != null
                ? null : queryContext.writeConfig();
        var dispatch = new LeafStageDispatcher.LeafDispatch(
                state.execution(), plan, boundPlan, splits, state.workerSelector(),
                state.dag(), state.dispatched(), writeConfig,
                !serialized.retrievalTopKHints().isEmpty(),
                Math.addExact(stage.currentGeneration(), 1L), null);
        return new PreparedStage(plan, dispatch);
    }

    private void awaitStageDispatch(
            Stage stage,
            StageNode plan,
            List<PartitionDispatch> partitions,
            DAGExecutionState state) {
        CompletableFuture.allOf(partitions.stream()
                        .map(PartitionDispatch::future)
                        .toArray(CompletableFuture[]::new))
                .thenRun(() -> finishStageDispatch(stage, plan, partitions, state))
                .exceptionally(failure -> {
                    failStageDispatch(stage, failure, state);
                    return null;
                });
    }

    private void finishStageDispatch(
            Stage stage,
            StageNode plan,
            List<PartitionDispatch> partitions,
            DAGExecutionState state) {
        leafStageDispatcher.populateDispatchRecords(
                partitions, stage, state.dispatched(), state.results());
        leafStageDispatcher.registerCohort(partitions, stage);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        leafStageDispatcher.registerTasks(partitions, stage, state.execution());
        leafStageDispatcher.restoreWinners(partitions, stage);
        if (state.durable() == null && "OUTPUT".equals(plan.taskType())) {
            if (partitions.size() != 1) {
                throw new IllegalStateException(
                        "OUTPUT stage requires a single result endpoint; stageId="
                                + stage.stageId() + " partitions=" + partitions.size());
            }
            state.resultReadySink().accept(state.results().get(stage.stageId()));
        }
        lifecycle.emitStageLineage(state.queryId(), plan, false);
        if (!state.completion().isDone() && !state.cancelled().get()
                && DAGCompletionEvaluator.allStagesFinished(state.dag())) {
            completeQuery(state);
        }
    }

    private void failStageDispatch(
            Stage stage, Throwable failure, DAGExecutionState state) {
        Throwable cause = failure instanceof java.util.concurrent.CompletionException
                && failure.getCause() != null ? failure.getCause() : failure;
        state.dispatchFailures().put(stage.stageId(), cause);
        if (!stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED)) {
            state.dispatchFailures().remove(stage.stageId(), cause);
            // A concurrent cancellation or terminal report already settled this stage.
            // Do not replace that outcome with a late dispatch callback.
        }
    }

    private void onStageFailed(
            Stage failed, DAGExecutionState state, Throwable dispatchFailure) {
        lifecycle.failStage(
                failed, state.dag(), state.dispatched(),
                state.completion(), state.queryId(), dispatchFailure);
    }

    private void completeQuery(DAGExecutionState state) {
        if (state.durable() != null) {
            lifecycle.completeDurable(state.queryId(), state.completion());
            return;
        }
        lifecycle.completeQuery(
                state.queryId(),
                state.results(), state.dag(), state.completion(), state.dispatched());
    }

    private static RuntimeException propagateSchedulingFailure(Exception failure) {
        return failure instanceof RuntimeException runtime ? runtime
                : new RuntimeException(
                        "Stage scheduling failed: " + failure.getMessage(), failure);
    }

    /** Returns whether every tracked stage for the query has finished. */
    @Override
    public boolean isQueryComplete(String queryId) {
        return lifecycle.isQueryComplete(queryId);
    }

    /** Completes deferred query finalization when the DAG reaches a terminal outcome. */
    @Override
    public void onQueryTerminal(String queryId, java.util.function.Consumer<Boolean> listener) {
        lifecycle.onQueryTerminal(queryId, listener);
    }

    /** Routes a worker terminal task report to its owning stage. */
    @Override
    public void onTaskTerminal(
            String queryId, String taskId, String state, ErrorDetail error) {
        lifecycle.onTaskTerminal(queryId, taskId, state, error);
    }

    /** Removes all per-query dispatch state. */
    public void cancelDurable(String executionId) {
        durableControl.cancel(executionId);
    }

    /** Removes all per-query dispatch state. */
    public void clearQuery(String queryId) {
        lifecycle.clearQuery(queryId);
    }

    /** Immutable dispatch metadata retained for an assigned task. */
    public record StageDispatchRecord(
            int stageId,
            DispatchResult result,
            int workerHttpPort,
            int workerTransportPort,
            org.pangrova.protocol.rest.task.SubmitTaskRequest request) {

        /** Creates a record when the serialized request is not retained. */
        public StageDispatchRecord(
                int stageId,
                DispatchResult result,
                int workerHttpPort,
                int workerTransportPort) {
            this(stageId, result, workerHttpPort, workerTransportPort, null);
        }
    }

    record PartitionDispatch(
            CompletableFuture<DispatchResult> future,
            int workerHttpPort,
            int workerTransportPort,
            String workerId,
            org.pangrova.protocol.rest.task.SubmitTaskRequest request,
            org.pangrova.protocol.v2.TaskReport winner) {}

    private record PreparedStage(
            StageNode plan, LeafStageDispatcher.LeafDispatch dispatch) {}

}
