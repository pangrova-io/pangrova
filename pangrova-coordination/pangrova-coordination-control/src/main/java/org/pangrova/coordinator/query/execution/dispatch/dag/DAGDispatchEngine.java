package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.DAGStagePlanBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGLifecycleController;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.BuildFilterGate;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest.StageRequestPlan;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.shuffle.api.coordinator.RecoverableShuffleControl;
import org.pangrova.shuffle.api.coordinator.ShuffleTaskAccess;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleTaskGrantBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry;
import org.pangrova.coordinator.query.task.QueryStageObservations;
import org.pangrova.query.profiling.enums.DispatchPhase;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.query.api.error.QueryFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives asynchronous event-based dispatch for multi-stage queries. */
@ThreadSafe
public final class DAGDispatchEngine implements StageCompletionCallback, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DAGDispatchEngine.class);

    private final org.pangrova.coordinator.task.TaskCancelRetrier cancelRetrier;
    private final PipelineConfig pipelineConfig;
    private final LeafStageDispatcher leafStageDispatcher;
    private final DAGStagePlanBinder stagePlanBinder;
    private final StageSplitFetcher stageSplitFetcher;
    private final CoalescedShuffleInputs coalescedShuffleInputs;
    private final DownstreamStageScheduler downstreamStageScheduler;
    private final LateProbeFilterDelivery lateProbeFilters;
    private final ScheduledExecutorService deadlines = ManagedExecutorFactory.newScheduledThreadPool(
            "dag-dispatch-deadline", 2);
    private final DAGLifecycleController lifecycle;
    private volatile QueryOperatorStatsRegistry operatorStats;
    private final org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DurableDAGControl durableControl;

    /** Creates a dispatch engine with the supplied execution dependencies. */
    public DAGDispatchEngine(
            DAGDispatchContext dependencies,
            List<StageLifecycleHandler> handlers,
            org.pangrova.coordinator.query.execution.split.StageSplitSource splitFetcher,
            PublishedGenerationBindingResolver artifactResolver,
            org.pangrova.coordinator.generation.read.GeneratedValueReadResolver generatedValueReads) {
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
        this.coalescedShuffleInputs = new CoalescedShuffleInputs(dependencies.adaptiveQueryConfig());
        this.downstreamStageScheduler = new DownstreamStageScheduler(dependencies.taskTracker());
        this.lateProbeFilters = new LateProbeFilterDelivery(dependencies.dispatcher(), dependencies.taskTracker());
        this.lifecycle = new DAGLifecycleController(
                dependencies.dispatcher(), canceller,
                dependencies.shuffleLifecycleManager(), dependencies.lineageEmitter(),
                handlers);
        this.leafStageDispatcher = new LeafStageDispatcher(
                dependencies.dispatcher(), dependencies.stateJournal(), dependencies.taskTimeoutSeconds(),
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

    /** Attaches the bounded diagnostics used by the query profile. */
    public void setOperatorStatsRegistry(QueryOperatorStatsRegistry registry) {
        operatorStats = registry;
    }

    /** The same exact-attempt cancellation queue is used by stage, query and speculative cleanup. */
    public org.pangrova.coordinator.task.TaskCancelRetrier cancelRetrier() {
        return cancelRetrier;
    }

    /**
     * Admits a task-level retry before its replacement is dispatched.
     * See {@link
     * org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGLifecycleController#tryPrepareTaskRetry}.
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
            scheduleRootStages(state, registerListeners(state));
        });
    }

    /** Schedules a live DAG and publishes its OUTPUT endpoint after Worker acknowledgement. */
    public CompletionStage<DispatchResult> start(
            QuerySession session,
            StageDAG dag,
            SqlCompiler compiler,
            WorkerSelector workerSelector,
            long timeoutMs,
            QueryExecutionScope queryContext,
            Consumer<DispatchResult> resultReadySink) {
        lifecycle.registerQuery(session.queryId(), dag, queryContext);
        var scope = lifecycle.queryContext(session.queryId());
        var filters = scope == null || scope.writeConfig() != null ? List.<BuildFilterGate>of()
                : BuildFilterGate.forLiveQuery(dag);
        DAGExecutionState state = DAGExecutionState.create(new StageExecutionContext.Live(session), dag,
                compiler, pipelineConfig.localShuffle() && !dag.stageEdges().isEmpty()
                        ? new org.pangrova.coordinator.scheduler.placement.LocalShufflePlacement(workerSelector)
                        : workerSelector, null, resultReadySink, filters);
        lifecycle.registerLiveExecution(state);
        try {
            scheduleRootStages(state, registerListeners(state));
            armDeadline(state.completion(), timeoutMs, () -> new PangrovaException(
                    PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT,
                    "DAG dispatch timed out [queryId=" + state.queryId() + "]"));
        } catch (RuntimeException | LinkageError failure) {
            state.completion().completeExceptionally(failure);
        }
        return state.completion().handle((result, failure) -> {
            if (failure == null) return result;
            if (failure instanceof CancellationException cancelled) throw cancelled;
            var dispatchFailure = DispatchCanceller.wrapIfNeeded(failure, state.queryId());
            try {
                lifecycle.failDispatch(state.queryId(), state.dag(), state.cancelled(), state.dispatched());
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != dispatchFailure) dispatchFailure.addSuppressed(cleanup);
            }
            throw dispatchFailure;
        });
    }

    /** Arms a deadline owned by the execution engine and removes it on completion. */
    public void armDeadline(CompletableFuture<?> completion, long timeoutMs,
            Supplier<? extends RuntimeException> failure) {
        if (completion.isDone()) return;
        var deadline = deadlines.schedule(() -> completion.completeExceptionally(failure.get()),
                timeoutMs, TimeUnit.MILLISECONDS);
        completion.whenComplete((result, caught) -> deadline.cancel(false));
    }

    private QueryStageObservations registerListeners(DAGExecutionState state) {
        var registry = operatorStats;
        var observations = registry != null && state.execution() instanceof StageExecutionContext.Live
                ? registry.beginExecution(state.queryId(), state.dag()) : null;
        if (observations != null) lifecycle.observeTaskSelections(state.queryId(), state.dag(), observations);
        for (Stage stage : state.dag().stageEntities()) {
            stage.stateMachine().addStateChangeListener((id, previous, next) -> {
                if (!lifecycle.owns(state.queryId(), state.dag()) || state.cancelled().get()) return;
                if (observations != null) {
                    observations.record(stage.stageId(), stage.currentGeneration(), previous, next);
                }
                updateRunningCount(previous, next, state.runningStageCount());
                try {
                    onStateChange(stage, next, state, observations);
                } catch (Exception | LinkageError failure) {
                    LOG.error("event=stage_listener_exception stageId={} queryId={}"
                                    + " transition={}→{} error={}",
                            id, state.queryId(), previous, next,
                            failure.getMessage(), failure);
                    state.cancelled().set(true);
                    state.completion().completeExceptionally(failure);
                }
            });
        }
        return observations;
    }

    private void onStateChange(
            Stage stage, StageState next, DAGExecutionState state, QueryStageObservations observations) {
        switch (next) {
            case RUNNING -> onStageRunning(state, observations);
            case FINISHED -> {
                fireHandlerEvent(stage, state, StageLifecycleHandler::onFinished);
                var filter = state.filterForBuild(stage.stageId());
                if (filter != null) {
                    filter.onFinished(stage);
                    lateProbeFilters.buildPublished(state, filter);
                }
                onStageFinished(state, observations);
            }
            case FAILED -> onFailedState(stage, state, observations);
            default -> {
            }
        }
    }

    private void onFailedState(Stage stage, DAGExecutionState state, QueryStageObservations observations) {
        fireHandlerEvent(stage, state, StageLifecycleHandler::onFailed);
        if (stage.stateMachine().getState() == StageState.PLANNED) {
            state.dispatchFailures().remove(stage.stageId());
            LOG.info("event=stage_retry_reschedule queryId={} stageId={} retryCount={}",
                    state.queryId(), stage.stageId(), stage.retryCount());
            scheduleStage(stage, state, observations);
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

    private void scheduleRootStages(DAGExecutionState state, QueryStageObservations observations) {
        List<Stage> roots = state.dag().rootStageEntities();
        if (roots.isEmpty()) {
            throw new IllegalStateException(
                    "DAG has no root stages for query " + state.queryId());
        }
        roots.forEach(stage -> scheduleStage(stage, state, observations));
        if (state.buildFilters().isEmpty()) return;

        long waitStarted = observations == null ? 0L : observations.elapsedNanos();
        long deadlineNanos = System.nanoTime() + BuildFilterGate.MAX_WAIT_NANOS;
        for (var gate : state.buildFilters()) {
            releaseFilteredProbe(gate, state, observations, waitStarted, deadlineNanos);
        }
    }

    private void releaseFilteredProbe(BuildFilterGate gate, DAGExecutionState state,
            QueryStageObservations observations, long waitStarted, long deadlineNanos) {
        gate.releaseProbe(state.completion(), deadlines, deadlineNanos).thenAccept(release -> {
            try {
                var probe = state.dag().findStage(gate.plan().probeStageId());
                if (observations != null) {
                    observations.recordRuntimeFilterWait(gate.plan().buildStageId(), probe.stageId(),
                            probe.currentGeneration(), waitStarted, release.outcome(), release.rangeCount());
                }
                if (!state.completion().isDone() && !state.cancelled().get()
                        && lifecycle.owns(state.queryId(), state.dag())) {
                    requestReadyDownstream(state, observations);
                }
            } catch (RuntimeException | LinkageError failure) {
                state.cancelled().set(true);
                state.completion().completeExceptionally(failure);
            }
        });
    }

    private void onStageRunning(DAGExecutionState state, QueryStageObservations observations) {
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
        requestReadyDownstream(state, observations);
    }

    private void onStageFinished(DAGExecutionState state, QueryStageObservations observations) {
        if (DAGCompletionEvaluator.allStagesFinished(state.dag())) {
            completeQuery(state);
        } else if (!state.cancelled().get()) {
            requestReadyDownstream(state, observations);
        }
    }

    private void requestReadyDownstream(DAGExecutionState state, QueryStageObservations observations) {
        var context = lifecycle.queryContext(state.queryId());
        if (state.durable() == null && context != null && context.writeConfig() == null) {
            downstreamStageScheduler.request(state, observations, () -> scheduleReadyDownstream(state, observations));
        } else {
            scheduleReadyDownstream(state, observations);
        }
    }

    @Override
    public void close() {
        try {
            downstreamStageScheduler.close();
        } finally {
            lateProbeFilters.close();
            deadlines.shutdownNow();
        }
    }

    private void scheduleReadyDownstream(DAGExecutionState state, QueryStageObservations observations) {
        for (var gate : state.buildFilters()) {
            int probeId = gate.plan().probeStageId();
            if (gate.blocksProbe() || !state.dag().edges().getOrDefault(probeId, List.of()).isEmpty()) continue;
            // Released roots can feed the producer/consumer pipeline despite the downstream cap.
            var probe = state.dag().findStage(probeId);
            if (probe.stateMachine().getState() == StageState.PLANNED) {
                scheduleStage(probe, state, observations);
            }
        }
        Set<Integer> activeIds = activeStageIds(state);
        for (Stage downstream : state.dag().readyStageEntities(activeIds)) {
            if (state.cancelled().get() || atConcurrencyLimit(state)) {
                return;
            }
            if (downstream.stateMachine().getState()
                    == StageState.PLANNED) {
                scheduleStage(downstream, state, observations);
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

    private void scheduleStage(Stage stage, DAGExecutionState state, QueryStageObservations observations) {
        var probeFilter = state.filterForProbe(stage.stageId());
        if (state.cancelled().get() || (probeFilter != null && probeFilter.blocksProbe())
                || !state.dag().admitInputs(stage.stageId())
                || !stage.stateMachine().tryTransitionTo(
                        StageState.SCHEDULING)) {
            return;
        }
        try {
            freezePhysicalInputs(stage, state);
            fireHandlerEvent(stage, state, StageLifecycleHandler::onScheduling);
            int generation = stage.currentGeneration();
            PreparedStage prepared = prepareStage(stage, state, generation, observations);
            List<PartitionDispatch> partitions = measureDispatchWork(
                    observations, stage.stageId(), generation, DispatchPhase.SUBMIT_TASKS,
                    () -> leafStageDispatcher.dispatchLeafStage(prepared.dispatch(), prepared.durablePlan()),
                    List::size);
            awaitStageDispatch(stage, prepared.plan(), partitions, state, observations);
        } catch (LinkageError failure) {
            failStageDispatch(stage, failure, state);
            throw DispatchCanceller.wrapIfNeeded(failure, state.queryId());
        } catch (Exception failure) {
            failStageDispatch(stage, failure, state);
            lifecycle.failQuery(state.queryId(), state.dag());
            throw propagateSchedulingFailure(failure);
        }
    }

    private void freezePhysicalInputs(Stage stage, DAGExecutionState state) {
        if (state.durable() != null) return;
        var context = lifecycle.queryContext(state.queryId());
        if (!isLiveRelationalRead(context)) {
            return;
        }
        var inputs = coalescedShuffleInputs.select(stage, state.dag());
        stage.freezeCoalescedInputs(inputs);
        if (inputs.isCoalesced()) {
            LOG.info("event=shuffle_input_coalesced queryId={} stageId={} sourcePartitions={} consumers={} inputs={}",
                    state.queryId(), stage.stageId(), inputs.originalCount(), inputs.coalescedCount(),
                    state.dag().edges().getOrDefault(stage.stageId(), List.of()).size());
        }
    }

    private static boolean isLiveRelationalRead(QueryExecutionScope context) {
        return context != null && context.writeConfig() == null && context.retrievalMerge().isEmpty()
                && context.retrievalArtifacts().isEmpty();
    }

    private PreparedStage prepareStage(Stage stage, DAGExecutionState state, int generation,
            QueryStageObservations observations) {
        if (state.durable() != null) {
            var durablePlan = state.durable().prepareRequestPlan(stage.plan());
            return new PreparedStage(stage.plan(), new LeafStageDispatcher.LeafDispatch(
                    state.execution(), stage.plan(), null, List.of(), null,
                    state.dag(), state.dispatched(), null, false,
                    state.durable().control().planEpoch(stage.stageId()), state.durable(), List.of()),
                    durablePlan);
        }
        StageNode plan = stage.plan();
        if (stage.retryCount() > 0) {
            state.dispatched().removeIf(record ->
                    record.stageId() == stage.stageId());
        }
        QueryExecutionScope queryContext = lifecycle.queryContext(state.queryId());
        lifecycle.emitStageLineage(state.queryId(), plan, true);
        boolean outputStage = state.dag().findDownstreamEdge(plan.stageId()) == null;
        List<String> visibleColumns = queryContext == null ? List.of() : queryContext.preOptColumnNames();
        var compiled = measureDispatchWork(observations, plan.stageId(), generation, DispatchPhase.SERIALIZE_PLAN,
                () -> state.compilePlan(plan, visibleColumns), null);
        if (queryContext != null && !queryContext.retrievalArtifacts().isEmpty()) {
            LOG.info("event=retrieval_stage_hints queryId={} stageId={} hints={}",
                    state.queryId(), plan.stageId(), compiled.retrievalTopKHints().stream()
                            .map(hint -> hint.kind() + ":" + hint.sourceKey()).toList());
        }
        boolean allowsRanges = !outputStage && plan.parallelism() > 1 && isLiveRelationalRead(queryContext)
                && compiled.retrievalTopKHints().isEmpty();
        var fetched = measureDispatchWork(observations, plan.stageId(), generation, DispatchPhase.ENUMERATE_SPLITS,
                () -> stageSplitFetcher.fetch(
                        plan, queryContext == null ? null : queryContext.requireLiveSession(),
                        queryContext == null ? Map.of() : queryContext.snapshotOverrides(),
                        queryContext == null || queryContext.writeConfig() == null
                                ? null : queryContext.writeConfig().writeMode(), state.filterForProbe(plan.stageId()),
                        allowsRanges ? FileGranularity.BYTE_RANGES : FileGranularity.WHOLE_OBJECT),
                result -> result.splits().size());
        List<QuerySplit> splits = fetched.splits();
        long planEpoch = Math.addExact(generation, 1L);
        var probeFilter = state.filterForProbe(plan.stageId());
        if (probeFilter != null) {
            probeFilter.bindProbeGeneration(
                    planEpoch, fetched.acceptsLateFilter() && isLiveRelationalRead(queryContext));
        }
        BoundExecutionPlan boundPlan = measureDispatchWork(
                observations, plan.stageId(), generation, DispatchPhase.VALIDATE_AND_BIND_PLAN, () -> {
                    org.pangrova.execution.split.CanonicalSplitValidator.validateAll(
                            () -> splits.stream().map(QuerySplit::data).iterator(), state.queryId(), plan.stageId());
                    return stagePlanBinder.bind(new DAGStagePlanBinder.Input(
                            compiled, plan, fetched, queryContext, state.session(),
                            state.workerSelector(), outputStage));
                }, null);
        // Only the terminal stage owns the table write. Upstream stages must
        // produce their ExchangeRel shuffle output with a shuffle task binding.
        var writeConfig = queryContext == null
                || state.dag().findDownstreamEdge(plan.stageId()) != null
                ? null : queryContext.writeConfig();
        var buildFilter = state.filterForBuild(stage.stageId());
        var dispatch = new LeafStageDispatcher.LeafDispatch(
                state.execution(), plan, boundPlan, splits, state.workerSelector(),
                state.dag(), state.dispatched(), writeConfig,
                !compiled.retrievalTopKHints().isEmpty(),
                planEpoch, null,
                buildFilter == null ? List.of() : buildFilter.buildOrdinals());
        return new PreparedStage(plan, dispatch, null);
    }

    private void awaitStageDispatch(
            Stage stage,
            StageNode plan,
            List<PartitionDispatch> partitions,
            DAGExecutionState state,
            QueryStageObservations observations) {
        int generation = stage.currentGeneration();
        long waitStarted = dispatchTimestamp(observations);
        var acknowledgements = CompletableFuture.allOf(partitions.stream()
                        .map(PartitionDispatch::future)
                        .toArray(CompletableFuture[]::new));
        acknowledgements
                .thenRun(() -> {
                    if (stage.currentGeneration() == generation) {
                        recordDispatchWork(observations, stage.stageId(), generation, DispatchPhase.OBSERVE_COHORT_ACK,
                                waitStarted, partitions.size(), true);
                        finishStageDispatch(stage, plan, partitions, state);
                    }
                })
                .exceptionally(failure -> {
                    if (stage.currentGeneration() == generation && lifecycle.owns(state.queryId(), state.dag())) {
                        if (acknowledgements.isCompletedExceptionally()) {
                            recordDispatchWork(observations, stage.stageId(), generation,
                                    DispatchPhase.OBSERVE_COHORT_ACK, waitStarted, partitions.size(), false);
                        }
                        failStageDispatch(stage, failure, state);
                    }
                    return null;
                });
    }

    /** Records a completed synchronous phase even when its original operation throws. */
    private static <T> T measureDispatchWork(QueryStageObservations observations, int stageId, int generation,
            DispatchPhase phase, Supplier<T> operation, Function<T, Integer> itemCount) {
        if (observations == null) return operation.get();
        long started = observations.elapsedNanos();
        T result;
        try {
            result = operation.get();
        } catch (RuntimeException | Error failure) {
            // Partial work may exist, but no complete result supplies a reliable item count.
            observations.recordDispatchWork(stageId, generation, phase, started, null, false);
            throw failure;
        }
        observations.recordDispatchWork(stageId, generation, phase, started,
                itemCount == null ? null : itemCount.apply(result), true);
        return result;
    }

    private static long dispatchTimestamp(QueryStageObservations observations) {
        return observations == null ? 0 : observations.elapsedNanos();
    }

    private static void recordDispatchWork(QueryStageObservations observations, int stageId, int generation,
            DispatchPhase phase, long start, Integer itemCount, boolean successful) {
        if (observations != null) {
            observations.recordDispatchWork(stageId, generation, phase, start, itemCount, successful);
        }
    }

    private void finishStageDispatch(
            Stage stage,
            StageNode plan,
            List<PartitionDispatch> partitions,
            DAGExecutionState state) {
        if (state.cancelled().get() || !lifecycle.owns(state.queryId(), state.dag())) return;
        leafStageDispatcher.populateDispatchRecords(
                partitions, stage, state.dispatched(), state.results());
        if (partitions.stream().anyMatch(partition -> partition.winner() == null
                && partition.request().taskRequest().getTaskKind()
                == org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE)) {
            stage.markWriteAttempt();
        }
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

    @Override
    public void onTaskRunning(String queryId, String taskId, int stageId) {
        var state = lifecycle.liveExecution(queryId);
        if (state == null) return;
        var gate = state.filterForProbe(stageId);
        if (gate != null) lateProbeFilters.offer(state, gate, taskId);
    }

    /** Routes a worker terminal task report to its owning stage. */
    @Override
    public void onTaskTerminal(
            String queryId, String taskId, String state, QueryFailure error) {
        lifecycle.onTaskTerminal(queryId, taskId, state, error);
    }

    @Override
    public void onSuccessfulTaskTerminal(String queryId, String logicalTaskId, SuccessfulAttempt attempt,
            Map<Integer, List<BuildKeyRange>> ranges) {
        lifecycle.onSuccessfulTaskTerminal(queryId, logicalTaskId, attempt, ranges);
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
            StageNode plan, LeafStageDispatcher.LeafDispatch dispatch, StageRequestPlan durablePlan) {}

}
