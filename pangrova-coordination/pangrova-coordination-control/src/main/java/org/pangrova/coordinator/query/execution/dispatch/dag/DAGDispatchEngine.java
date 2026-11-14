package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.query.api.task.QuerySplit;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveStagePlanResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.DAGStagePlanBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGLifecycleController;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.shuffle.api.coordinator.RecoverableShuffleControl;
import org.pangrova.shuffle.api.coordinator.ShuffleTaskAccess;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleTaskGrantBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.query.api.error.QueryFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives asynchronous event-based dispatch for multi-stage queries. */
@ThreadSafe
public final class DAGDispatchEngine implements StageCompletionCallback {
    private static final Logger LOG = LoggerFactory.getLogger(DAGDispatchEngine.class);

    private final org.pangrova.coordinator.task.TaskCancelRetrier cancelRetrier;
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
            org.pangrova.coordinator.query.execution.split.StageSplitSource splitFetcher,
            PublishedGenerationBindingResolver artifactResolver,
            org.pangrova.coordinator.generation.read.GeneratedValueReadResolver generatedValueReads) {
        this(dependencies, handlers, splitFetcher, artifactResolver, generatedValueReads, null);
    }

    /** Creates a dispatch engine with the supplied execution dependencies. */
    public DAGDispatchEngine(
            DAGDispatchContext dependencies,
            List<StageLifecycleHandler> handlers,
            org.pangrova.coordinator.query.execution.split.StageSplitSource splitFetcher,
            PublishedGenerationBindingResolver artifactResolver,
            org.pangrova.coordinator.generation.read.GeneratedValueReadResolver generatedValueReads,
            org.pangrova.planner.analysis.SubPlanReoptimizer subPlanReoptimizer) {
        DispatchCanceller canceller = new DispatchCanceller(
                dependencies.dispatcher(), dependencies.taskTracker());
        this.cancelRetrier = canceller.cancelRetrier();
        this.pipelineConfig = dependencies.pipelineConfig();
        var externalShuffle = dependencies.shuffleLifecycleManager()
                instanceof RecoverableShuffleControl control ? control : null;
        if ((artifactResolver == null) != (generatedValueReads == null)) {
            throw new IllegalArgumentException("Published and generated value readers must be configured together");
        }
        this.stagePlanBinder = new DAGStagePlanBinder(new RetrievalGenerationResolver(artifactResolver),
                generatedValueReads);
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
                        artifactResolver, pipelineConfig.localShuffle(),
                        externalShuffle instanceof ExternalShuffleDescriptorEncoder encoder ? encoder : null,
                        dependencies.shuffleLifecycleManager()
                                instanceof ShuffleTaskAccess,
                        dependencies.shuffleLifecycleManager()
                                instanceof ShuffleTaskAccess grants
                                        ? new CoordinatorShuffleTaskGrantBinder(grants)::bind
                                        : java.util.function.UnaryOperator.identity()));
        this.durableControl = new org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DurableDAGControl(
                lifecycle, externalShuffle);
    }

    /** The same exact-attempt cancellation queue is used by stage, query and speculative cleanup. */
    public org.pangrova.coordinator.task.TaskCancelRetrier cancelRetrier() {
        return cancelRetrier;
    }

    /**
     * Admits a task-level retry before its replacement is dispatched.
     * See {@link org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGLifecycleController#tryPrepareTaskRetry}.
     */
    public boolean tryPrepareTaskRetry(String queryId, String failedTaskId,
            String retryTaskId, int maxRetries) {
        return lifecycle.tryPrepareTaskRetry(queryId, failedTaskId, retryTaskId, maxRetries);
    }

    /** Reverts an uncommitted retry reservation after dispatch failed. */
    public void rollbackTaskRetry(String queryId, String failedTaskId, String retryTaskId) {
        lifecycle.rollbackTaskRetry(queryId, failedTaskId, retryTaskId);
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
            QuerySession session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            QueryExecutionScope queryContext) {
        return dispatch(session, dag, compiler, workerSelector, timeoutMs,
                queryContext, ignored -> { });
    }

    /** Publishes the OUTPUT endpoint once Worker dispatch is acknowledged. */
    public DispatchResult dispatch(
            QuerySession session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            QueryExecutionScope queryContext,
            Consumer<DispatchResult> resultReadySink) {
        lifecycle.registerQuery(session.queryId(), dag, queryContext);
        return dispatchRegistered(session, dag, compiler, workerSelector, timeoutMs, resultReadySink);
    }

    /** Dispatches a DAG using any previously registered query context. */
    public DispatchResult dispatch(
            QuerySession session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs) {
        lifecycle.registerDagIfAbsent(session.queryId(), dag);
        return dispatchRegistered(session, dag, compiler, workerSelector, timeoutMs, ignored -> { });
    }

    private DispatchResult dispatchRegistered(
            QuerySession session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            Consumer<DispatchResult> resultReadySink) {
        DAGExecutionState state = DAGExecutionState.create(new StageExecutionContext.Live(session), dag,
                compiler, pipelineConfig.localShuffle() && !dag.stageEdges().isEmpty()
                        ? new org.pangrova.coordinator.scheduler.placement.LocalShufflePlacement(workerSelector)
                        : workerSelector, null, resultReadySink);
        lifecycle.registerLiveExecution(state);
        try {
            registerListeners(state);
            scheduleRootStages(state);
            return DAGCompletionEvaluator.awaitCompletion(
                    state.completion(), timeoutMs, session.queryId());
        } catch (Exception failure) {
            lifecycle.failDispatch(
                    state.queryId(), state.dag(), state.cancelled(), state.dispatched());
            throw failure;
        }
    }

    private void registerListeners(DAGExecutionState state) {
        for (Stage stage : state.dag().stageEntities()) {
            stage.stateMachine().addStateChangeListener((id, previous, next) -> {
                if (!lifecycle.owns(state.queryId(), state.dag()) || state.cancelled().get()) return;
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
            Stage stage, StageState next, DAGExecutionState state) {
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
        if (stage.stateMachine().getState() == StageState.PLANNED) {
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
            StageState previous,
            StageState next,
            AtomicInteger runningCount) {
        if (next == StageState.RUNNING) {
            runningCount.incrementAndGet();
        } else if (previous == StageState.RUNNING
                && isRunningExit(next)) {
            runningCount.decrementAndGet();
        }
    }

    private static boolean isRunningExit(StageState state) {
        return state == StageState.FLUSHING
                || state == StageState.FINISHED
                || state == StageState.FAILED
                || state == StageState.CANCELLED;
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
                    == StageState.PLANNED) {
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
                .filter(stage -> stage.stateMachine().getState() == StageState.FINISHED)
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
        if (state.cancelled().get() || !state.dag().admitInputs(stage.stageId())
                || !stage.stateMachine().tryTransitionTo(
                        StageState.SCHEDULING)) {
            return;
        }
        fireHandlerEvent(stage, state, StageLifecycleHandler::onScheduling);
        try {
            PreparedStage prepared = prepareStage(stage, state);
            List<PartitionDispatch> partitions =
                    leafStageDispatcher.dispatchLeafStage(prepared.dispatch());
            awaitStageDispatch(stage, prepared.plan(), partitions, state);
        } catch (Exception failure) {
            stage.stateMachine().tryTransitionTo(StageState.FAILED);
            lifecycle.failQuery(state.queryId(), state.dag());
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
        QueryExecutionScope queryContext = lifecycle.queryContext(state.queryId());
        lifecycle.emitStageLineage(state.queryId(), plan, true);
        SerializedPlanFragment serialized = state.serializePlan(
                plan, queryContext == null ? List.of() : queryContext.preOptColumnNames());
        if (queryContext != null && !queryContext.retrievalArtifacts().isEmpty()) {
            LOG.info("event=retrieval_stage_hints queryId={} stageId={} hints={}",
                    state.queryId(), plan.stageId(), serialized.retrievalTopKHints().stream()
                            .map(hint -> hint.kind() + ":" + hint.sourceKey()).toList());
        }
        List<QuerySplit> splits = stageSplitFetcher.fetch(
                plan, queryContext == null ? null : queryContext.requireLiveSession(),
                queryContext == null ? Map.of() : queryContext.snapshotOverrides(),
                queryContext == null || queryContext.writeConfig() == null
                        ? null : queryContext.writeConfig().writeMode());
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
        int generation = stage.currentGeneration();
        CompletableFuture.allOf(partitions.stream()
                        .map(PartitionDispatch::future)
                        .toArray(CompletableFuture[]::new))
                .thenRun(() -> {
                    if (stage.currentGeneration() == generation) finishStageDispatch(stage, plan, partitions, state);
                })
                .exceptionally(failure -> {
                    if (stage.currentGeneration() == generation && lifecycle.owns(state.queryId(), state.dag())) {
                        failStageDispatch(stage, failure, state);
                    }
                    return null;
                });
    }

    private void finishStageDispatch(
            Stage stage,
            StageNode plan,
            List<PartitionDispatch> partitions,
            DAGExecutionState state) {
        if (state.cancelled().get() || !lifecycle.owns(state.queryId(), state.dag())) return;
        leafStageDispatcher.populateDispatchRecords(
                partitions, stage, state.dispatched(), state.results());
        if (partitions.stream().anyMatch(partition -> partition.winner() == null && partition.request().taskRequest().getTaskKind()
                == org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE)) stage.markWriteAttempt();
        if (state.durable() == null && "OUTPUT".equals(plan.taskType())) {
            if (partitions.size() != 1) {
                throw new IllegalStateException(
                        "OUTPUT stage requires a single result endpoint; stageId="
                                + stage.stageId() + " partitions=" + partitions.size());
            }
            var acknowledged = state.results().get(stage.stageId());
            if (acknowledged.resultEndpoint() != null) stage.markClientResultPublished();
            state.resultReadySink().accept(acknowledged);
        }
        leafStageDispatcher.registerCohort(partitions, stage);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        leafStageDispatcher.registerTasks(partitions, stage, state.execution());
        leafStageDispatcher.restoreWinners(partitions, stage);
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
        if (!stage.stateMachine().tryTransitionTo(StageState.FAILED)) {
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
            lifecycle.completeDurable(state.queryId(), state.dag(), state.completion());
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

    @Override
    public boolean ownsQueryCompletion(String queryId) {
        return lifecycle.ownsQueryCompletion(queryId);
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
            String queryId, String taskId, String state, QueryFailure error) {
        lifecycle.onTaskTerminal(queryId, taskId, state, error);
    }

    /** Removes all per-query dispatch state. */
    public void cancelDurable(String executionId) {
        durableControl.cancel(executionId);
    }

    /** Ends logical dispatch and finalization without retiring physical attempt ownership. */
    public void cancelLiveDispatch(String queryId) {
        lifecycle.cancelLiveDispatch(queryId);
    }

    /** Removes all per-query dispatch state. */
    public void clearQuery(String queryId) {
        lifecycle.clearQuery(queryId);
    }

    /** Immutable dispatch metadata retained for an assigned task. */
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
