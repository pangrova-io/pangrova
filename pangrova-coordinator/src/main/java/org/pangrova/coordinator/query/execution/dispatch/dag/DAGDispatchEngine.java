package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveDecision;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskFaultHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives async event-driven DAG dispatch for multi-stage queries.
 * Handles DAG traversal, event dispatch, and shuffle lifecycle cleanup.
 * Retry, adaptive query, and speculation concerns are delegated to
 * {@link StageLifecycleHandler} implementations.
 *
 * <p>Concurrency: shared across queries. Per-query state is method-local
 * ({@link ConcurrentHashMap}/{@link AtomicInteger}). Instance field references
 * are immutable after construction (no reassignment); the objects they point to
 * (e.g. handlers, LeafStageDispatcher) may hold mutable internal state guarded
 * by their own concurrency strategies.</p>
 */
@ThreadSafe
public final class DAGDispatchEngine {
    private static final Logger LOG = LoggerFactory.getLogger(DAGDispatchEngine.class);

    private final org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher;
    private final DispatchCanceller canceller;
    private final CatalogRegistry catalogRegistry;
    private final org.pangrova.governance.lineage.LineageEventEmitter lineageEmitter;
    private final PipelineConfig pipelineConfig;
    private final List<StageLifecycleHandler> handlers;
    private final org.pangrova.coordinator.index.IndexAwareSplitFetcher indexAwareSplitFetcher;
    private final LeafStageDispatcher leafStageDispatcher;
    private final org.pangrova.shuffle.ShuffleLifecycleManager shuffleLifecycleManager;
    private final org.pangrova.planner.SubPlanReoptimizer subPlanReoptimizer;
    private final TaskFaultHandler taskFaultHandler = new TaskFaultHandler();

    /**
     * Unified task slot map that eliminates the race between
     * {@link #registerTask} and {@link #onTaskTerminal}.
     *
     * <p>Previously two separate maps ({@code taskStageRegistry} and
     * {@code earlyTerminalEvents}) were used, but atomic operations on
     * one map could not be composed atomically with operations on the
     * other. Using a single map with {@link ConcurrentHashMap#compute}
     * makes the check-and-act atomic per task key.</p>
     *
     * <p>A slot transitions through: absent → {@code EARLY_ONLY} (terminal
     * arrived first) → consumed by registerTask, or absent → {@code STAGE_ONLY}
     * (registered first) → consumed by onTaskTerminal.</p>
     */
    private final ConcurrentHashMap<String, TaskSlot> taskSlots = new ConcurrentHashMap<>();

    /** Maps queryId to its taskIds for bulk cleanup on query terminal. */
    private final ConcurrentHashMap<String, Set<String>> queryTaskIndex = new ConcurrentHashMap<>();

    /** Per-query context carrying snapshot overrides, DML config, etc. */
    private final ConcurrentHashMap<String, DAGQueryContext> queryContextIndex = new ConcurrentHashMap<>();

    /** Per-query DAG for completion checking by TaskStatusReceiver. */
    private final ConcurrentHashMap<String, StageDAG> queryDagIndex = new ConcurrentHashMap<>();

    public DAGDispatchEngine(DAGDispatchContext deps, List<StageLifecycleHandler> handlers,
                             org.pangrova.coordinator.index.IndexAwareSplitFetcher indexAwareSplitFetcher) {
        this(deps, handlers, indexAwareSplitFetcher, null);
    }

    public DAGDispatchEngine(DAGDispatchContext deps, List<StageLifecycleHandler> handlers,
                             org.pangrova.coordinator.index.IndexAwareSplitFetcher indexAwareSplitFetcher,
                             org.pangrova.planner.SubPlanReoptimizer subPlanReoptimizer) {
        this.dispatcher = deps.dispatcher();
        this.canceller = new DispatchCanceller(
                deps.dispatcher(), deps.taskTracker(), deps.taskTimeoutSeconds());
        this.catalogRegistry = deps.catalogRegistry();
        this.lineageEmitter = deps.lineageEmitter();
        this.pipelineConfig = deps.pipelineConfig();
        this.handlers = List.copyOf(handlers);
        this.indexAwareSplitFetcher = indexAwareSplitFetcher;
        this.shuffleLifecycleManager = deps.shuffleLifecycleManager();
        this.subPlanReoptimizer = subPlanReoptimizer;
        this.leafStageDispatcher = new LeafStageDispatcher(
                deps.dispatcher(), deps.runtimeFilterRegistry(),
                deps.stateJournal(), this.canceller,
                this::registerTask);
    }

    /**
     * Dispatches with additional query context (snapshot overrides, DML config, etc.).
     */
    public DispatchResult dispatch(CoordinatorSessionContext session, StageDAG dag,
                                   SqlCompiler compiler, WorkerSelector workerSelector,
                                   long timeoutMs, DAGQueryContext queryContext) {
        if (queryContext != null) {
            queryContextIndex.put(session.queryId(), queryContext);
        }
        queryDagIndex.put(session.queryId(), dag);
        return dispatch(session, dag, compiler, workerSelector, timeoutMs);
    }

    public DispatchResult dispatch(CoordinatorSessionContext session, StageDAG dag,
                                   SqlCompiler compiler, WorkerSelector workerSelector,
                                   long timeoutMs) {
        queryDagIndex.putIfAbsent(session.queryId(), dag);
        List<StageDispatchRecord> allDispatched = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean cancelled =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        try {
            return executeAsync(
                    session, dag, compiler, workerSelector,
                    allDispatched, timeoutMs, cancelled);
        } catch (Exception e) {
            cancelled.set(true);
            canceller.cancelAllTasks(session.queryId(), allDispatched);
            shuffleLifecycleManager.cleanupShuffle(session.queryId());
            fireQueryFailed(session.queryId());
            throw e;
        }
    }

    private DispatchResult executeAsync(
            CoordinatorSessionContext session, StageDAG dag,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched, long timeoutMs,
            java.util.concurrent.atomic.AtomicBoolean cancelled) {
        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();
        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();
        AtomicInteger runningStageCount = new AtomicInteger(0);
        registerListeners(dag, session, compiler, ws, dispatched,
                results, completion, cancelled, runningStageCount);
        scheduleRootStages(dag, session, compiler, ws, dispatched,
                results, completion, cancelled, runningStageCount);
        return DAGCompletionEvaluator.awaitCompletion(completion, timeoutMs, session.queryId());
    }

    private void registerListeners(
            StageDAG dag, CoordinatorSessionContext session,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> completion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        for (Stage stage : dag.stageEntities()) {
            stage.stateMachine().addStateChangeListener((id, old, next) -> {
                updateRunningCount(old, next, runningStageCount);
                try {
                    if (next == StageStateMachine.State.RUNNING) {
                        onStageRunning(stage, dag, session, compiler, ws,
                                dispatched, results, completion, cancelled,
                                runningStageCount);
                    } else if (next == StageStateMachine.State.FINISHED) {
                        fireHandlerEvent(stage, dag, session,
                                StageLifecycleHandler::onFinished);
                        onStageFinished(stage, dag, session, compiler, ws,
                                dispatched, results, completion, cancelled,
                                runningStageCount);
                    } else if (next == StageStateMachine.State.FAILED) {
                        fireHandlerEvent(stage, dag, session,
                                StageLifecycleHandler::onFailed);
                        // RetryStageHandler may have reset the stage to PLANNED
                        // for re-scheduling. If so, skip failure propagation and
                        // re-schedule the stage instead of failing the query.
                        if (stage.stateMachine().getState()
                                == StageStateMachine.State.PLANNED) {
                            LOG.info("event=stage_retry_reschedule queryId={}"
                                            + " stageId={} retryCount={}",
                                    session.queryId(), stage.stageId(),
                                    stage.retryCount());
                            scheduleStage(stage, session, dag, compiler, ws,
                                    dispatched, results, completion, cancelled,
                                    runningStageCount);
                        } else {
                            cancelled.set(true);
                            onStageFailed(stage, dag, dispatched,
                                    completion, session.queryId());
                        }
                    }
                } catch (Exception e) {
                    LOG.error("event=stage_listener_exception stageId={} queryId={}"
                                    + " transition={}→{} error={}",
                            id, session.queryId(), old, next, e.getMessage(), e);
                    cancelled.set(true);
                    completion.completeExceptionally(e);
                }
            });
        }
    }

    private static void updateRunningCount(
            StageStateMachine.State old, StageStateMachine.State next,
            AtomicInteger runningStageCount) {
        if (next == StageStateMachine.State.RUNNING) {
            runningStageCount.incrementAndGet();
        } else if (old == StageStateMachine.State.RUNNING
                && (next == StageStateMachine.State.FLUSHING
                    || next == StageStateMachine.State.FINISHED
                    || next == StageStateMachine.State.FAILED
                    || next == StageStateMachine.State.CANCELLED)) {
            runningStageCount.decrementAndGet();
        }
    }

    private void fireHandlerEvent(
            Stage stage, StageDAG dag, CoordinatorSessionContext session,
            java.util.function.BiConsumer<StageLifecycleHandler, StageLifecycleEvent> method) {
        var event = new StageLifecycleEvent(stage, dag, session.queryId(), session);
        for (StageLifecycleHandler handler : handlers) {
            try {
                method.accept(handler, event);
            } catch (Exception e) {
                LOG.warn("event=handler_exception handler={} queryId={} stageId={} error={}",
                        handler.getClass().getSimpleName(), session.queryId(),
                        stage.stageId(), e.getMessage(), e);
            }
            // If a handler (e.g. RetryStageHandler) reset the stage to PLANNED
            // for re-scheduling, stop processing remaining handlers — they should
            // not act on a stage that is no longer in the expected terminal state.
            if (stage.stateMachine().getState() == StageStateMachine.State.PLANNED) {
                LOG.info("event=handler_chain_interrupted queryId={} stageId={}"
                                + " reason=stage_reset_to_planned handler={}",
                        session.queryId(), stage.stageId(),
                        handler.getClass().getSimpleName());
                break;
            }
        }
    }

    private void fireQueryCompleted(String queryId) {
        clearQuery(queryId);
        for (StageLifecycleHandler handler : handlers) {
            try {
                handler.onQueryCompleted(queryId);
            } catch (Exception e) {
                LOG.warn("event=handler_exception handler={} queryId={} lifecycle=query_completed error={}",
                        handler.getClass().getSimpleName(), queryId, e.getMessage(), e);
            }
        }
    }

    private void fireQueryFailed(String queryId) {
        clearQuery(queryId);
        for (StageLifecycleHandler handler : handlers) {
            try {
                handler.onQueryFailed(queryId);
            } catch (Exception e) {
                LOG.warn("event=handler_exception handler={} queryId={} lifecycle=query_failed error={}",
                        handler.getClass().getSimpleName(), queryId, e.getMessage(), e);
            }
        }
    }

    private void scheduleRootStages(
            StageDAG dag, CoordinatorSessionContext session,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> completion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        List<Stage> roots = dag.rootStageEntities();
        if (roots.isEmpty()) {
            throw new IllegalStateException(
                    "DAG has no root stages for query " + session.queryId());
        }
        for (Stage root : roots) {
            scheduleStage(root, session, dag, compiler, ws, dispatched,
                    results, completion, cancelled, runningStageCount);
        }
    }

    private void onStageRunning(
            Stage running, StageDAG dag, CoordinatorSessionContext session,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> completion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        if (!pipelineConfig.enabled() || cancelled.get()) {
            return;
        }
        if (pipelineConfig.maxConcurrentStages() > 0
                && runningStageCount.get() >= pipelineConfig.maxConcurrentStages()) {
            LOG.debug("event=pipeline_backpressure queryId={} runningCount={}"
                            + " maxConcurrent={}",
                    session.queryId(), runningStageCount.get(),
                    pipelineConfig.maxConcurrentStages());
            return;
        }
        scheduleReadyDownstream(running.stageId(), dag, session, compiler, ws,
                dispatched, results, completion, cancelled, runningStageCount);
    }

    private void onStageFinished(
            Stage finished, StageDAG dag, CoordinatorSessionContext session,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> completion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        if (DAGCompletionEvaluator.allStagesFinished(dag)) {
            fireQueryCompleted(session.queryId());
            DAGCompletionEvaluator.completeWithLastResult(results, dag, completion);
            return;
        }
        if (cancelled.get()) {
            return;
        }
        scheduleReadyDownstream(-1, dag, session, compiler, ws,
                dispatched, results, completion, cancelled, runningStageCount);
    }

    private void scheduleReadyDownstream(
            int triggerStageId, StageDAG dag,
            CoordinatorSessionContext session, SqlCompiler compiler,
            WorkerSelector ws, List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> completion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        Set<Integer> activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        for (Stage downstream : dag.readyStageEntities(activeIds)) {
            if (cancelled.get()) {
                return;
            }
            if (pipelineConfig.maxConcurrentStages() > 0
                    && runningStageCount.get() >= pipelineConfig.maxConcurrentStages()) {
                return;
            }
            if (downstream.stateMachine().getState() == StageStateMachine.State.PLANNED) {
                if (triggerStageId >= 0) {
                    LOG.info("event=pipeline_schedule queryId={} triggerStageId={}"
                                    + " downstreamStageId={}",
                            session.queryId(), triggerStageId, downstream.stageId());
                }
                scheduleStage(downstream, session, dag, compiler, ws,
                        dispatched, results, completion, cancelled, runningStageCount);
            }
        }
        // Fallback completion check: covers the race where all stages finished
        // between the onStageFinished check and this point, with no new stages
        // to schedule. Without this, the completion future could hang indefinitely.
        if (!completion.isDone() && !cancelled.get()
                && DAGCompletionEvaluator.allStagesFinished(dag)) {
            fireQueryCompleted(session.queryId());
            DAGCompletionEvaluator.completeWithLastResult(results, dag, completion);
        }
    }

    private void onStageFailed(Stage failed, StageDAG dag,
                               List<StageDispatchRecord> dispatched,
                               CompletableFuture<DispatchResult> completion,
                               String queryId) {
        cancelDownstreamStages(failed, dag, dispatched);
        fireQueryFailed(queryId);
        org.pangrova.protocol.rest.error.ErrorDetail err = failed.lastError();
        String msg = err != null ? err.message()
                : "Stage " + failed.stageId() + " failed";
        completion.completeExceptionally(
                new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED, msg));
    }

    private void cancelDownstreamStages(Stage failed, StageDAG dag,
                                        List<StageDispatchRecord> dispatched) {
        Set<Integer> toCancelIds = new HashSet<>();
        collectTransitiveDownstream(failed.stageId(), dag, toCancelIds);
        for (Stage stage : dag.stageEntities()) {
            if (!toCancelIds.contains(stage.stageId())) {
                continue;
            }
            StageStateMachine.State state = stage.stateMachine().getState();
            if (state == StageStateMachine.State.FINISHED
                    || state == StageStateMachine.State.FAILED
                    || state == StageStateMachine.State.CANCELLED) {
                continue;
            }
            if (state == StageStateMachine.State.PLANNED) {
                stage.stateMachine().tryTransitionTo(StageStateMachine.State.CANCELLED);
            } else {
                stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
            }
            cancelStageTasks(stage, dispatched);
        }
    }

    private static void collectTransitiveDownstream(int rootStageId, StageDAG dag,
                                                    Set<Integer> result) {
        java.util.ArrayDeque<Integer> queue = new java.util.ArrayDeque<>();
        queue.add(rootStageId);
        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (int downstream : dag.directDownstream(current)) {
                if (result.add(downstream)) {
                    queue.add(downstream);
                }
            }
        }
    }

    private void cancelStageTasks(Stage stage, List<StageDispatchRecord> dispatched) {
        for (String taskId : stage.taskIds()) {
            StageDispatchRecord rec = DAGCompletionEvaluator.findDispatchRecord(taskId, dispatched);
            if (rec != null) {
                dispatcher.cancelTask(taskId, rec.result().workerHost(), rec.workerHttpPort());
            }
        }
    }

    private void scheduleStage(
            Stage stage, CoordinatorSessionContext session, StageDAG dag,
            SqlCompiler compiler, WorkerSelector ws,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results,
            CompletableFuture<DispatchResult> queryCompletion,
            java.util.concurrent.atomic.AtomicBoolean cancelled,
            AtomicInteger runningStageCount) {
        if (cancelled.get()) {
            return;
        }
        if (!stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING)) {
            return;
        }
        fireHandlerEvent(stage, dag, session, StageLifecycleHandler::onScheduling);
        // Apply AQE adaptive decisions (parallelism, coalescing, skew, reopt)
        // from completed upstream stages to this downstream stage.
        StageNode plan = applyAdaptiveDecisions(stage.plan(), dag);
        // On stage retry, remove stale dispatch records from previous attempts.
        // Without this, ExchangeSourceBuilder.buildExchangeSources would find
        // both old (failed) and new records for the same stageId, creating
        // duplicate exchange sources pointing to dead tasks.
        if (stage.retryCount() > 0) {
            dispatched.removeIf(rec -> rec.stageId() == stage.stageId());
        }
        DAGQueryContext qctx = queryContextIndex.get(session.queryId());
        try {
            emitStageLineage(session.queryId(), plan, true);
            List<String> columnNames = plan.planFragment().fieldNames();
            boolean isOutputStage = dag.findDownstreamEdge(plan.stageId()) == null;
            if (isOutputStage && qctx != null
                    && !qctx.preOptColumnNames().isEmpty()) {
                columnNames = qctx.preOptColumnNames();
            }
            byte[] planBytes = plan.planFragment().toSubstraitBytes(compiler, columnNames,
                    qctx != null ? qctx.originalSql() : null);
            List<SplitFetcher.SplitWithPaths> splitsWithPaths = fetchSplits(
                    plan, dag, session.queryId());
            org.pangrova.protocol.rest.config.WriteConfig wc =
                    qctx != null ? qctx.writeConfig() : null;
            List<PartitionDispatch> pds = leafStageDispatcher.dispatchLeafStage(
                    session, plan, planBytes, splitsWithPaths, ws, dag,
                    dispatched, wc);
            CompletableFuture.allOf(
                    pds.stream().map(PartitionDispatch::future)
                            .toArray(CompletableFuture[]::new))
                    .thenRun(() -> {
                        // Phase 1: populate dispatch records and results BEFORE
                        // RUNNING transition. The RUNNING listener triggers
                        // pipeline scheduling of downstream stages, which call
                        // buildExchangeSources() to find upstream dispatch
                        // records in `dispatched`.
                        leafStageDispatcher.populateDispatchRecords(
                                pds, stage, dispatched, results);
                        stage.stateMachine().tryTransitionTo(
                                StageStateMachine.State.RUNNING);
                        // Phase 2: register tasks AFTER RUNNING transition.
                        // registerTask() may replay buffered early-terminal
                        // events, which transition the stage RUNNING → FLUSHING
                        // → FINISHED. This is only valid from RUNNING state.
                        leafStageDispatcher.registerTasks(pds, stage, session);
                        LOG.info("event=thenrun_collect_done queryId={} stageId={} resultsSize={}",
                                session.queryId(), stage.stageId(), results.size());
                        emitStageLineage(session.queryId(), plan, false);
                        if (!queryCompletion.isDone() && !cancelled.get()
                                && DAGCompletionEvaluator.allStagesFinished(dag)) {
                            LOG.info("event=thenrun_safety_complete queryId={} stageId={}",
                                    session.queryId(), stage.stageId());
                            fireQueryCompleted(session.queryId());
                            DAGCompletionEvaluator.completeWithLastResult(
                                    results, dag, queryCompletion);
                        }
                    })
                    .exceptionally(ex -> {
                        Throwable cause = ex instanceof java.util.concurrent.CompletionException
                                ? ex.getCause() : ex;
                        stage.stateMachine().tryTransitionTo(
                                StageStateMachine.State.FAILED);
                        fireQueryFailed(session.queryId());
                        queryCompletion.completeExceptionally(cause);
                        return null;
                    });
        } catch (Exception e) {
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
            fireQueryFailed(session.queryId());
            throw e instanceof RuntimeException re ? re
                    : new RuntimeException(
                            "Stage scheduling failed: " + e.getMessage(), e);
        }
    }

    /**
     * Applies AQE adaptive decisions from upstream stages to the downstream plan.
     * Consumes: dynamic parallelism, partition coalescing.
     * Logs: skew detection and join strategy re-optimization (not yet actionable).
     */
    private StageNode applyAdaptiveDecisions(StageNode plan, StageDAG dag) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(plan.stageId(), List.of());
        if (upstreamIds.isEmpty()) {
            return plan;
        }
        for (int upstreamId : upstreamIds) {
            Stage upstream = dag.findStage(upstreamId);
            if (upstream == null) {
                continue;
            }
            AdaptiveDecision decision = upstream.adaptiveDecision();
            if (decision == null || !decision.hasAdjustments()) {
                continue;
            }
            if (decision.coalesceResult().isCoalesced()) {
                int coalesced = decision.coalesceResult().coalescedCount();
                LOG.info("event=aqe_coalesce_applied stageId={} upstreamStageId={}"
                        + " originalParallelism={} coalescedParallelism={}",
                        plan.stageId(), upstreamId, plan.parallelism(), coalesced);
                plan = plan.withParallelism(coalesced);
            }
            if (decision.parallelismChanged()) {
                int adjusted = decision.adjustedParallelism();
                LOG.info("event=aqe_parallelism_applied stageId={} upstreamStageId={}"
                        + " originalParallelism={} adjustedParallelism={}",
                        plan.stageId(), upstreamId, plan.parallelism(), adjusted);
                plan = plan.withParallelism(adjusted);
            }
            if (decision.skewResult().hasSkew()) {
                int skewedCount = decision.skewResult().skewedPartitions().size();
                int boosted = Math.min(
                        plan.parallelism() + skewedCount,
                        Math.max(1024, plan.parallelism() * 2));
                LOG.info("event=aqe_skew_mitigated stageId={} upstreamStageId={}"
                        + " skewedPartitions={} originalParallelism={} adjustedParallelism={}",
                        plan.stageId(), upstreamId, skewedCount,
                        plan.parallelism(), boosted);
                plan = plan.withParallelism(boosted);
            }
            if (decision.reOptimizationTriggered()) {
                if (subPlanReoptimizer != null) {
                    // F-AQE-03: thread the join-strategy hint into the reoptimizer
                    // so the cost model actually re-decides instead of producing
                    // an equivalent plan from unchanged statistics.
                    org.pangrova.planner.PlanFragment reoptimized =
                            decision.newJoinStrategy().isPresent()
                                    ? subPlanReoptimizer.reoptimize(
                                            plan.planFragment(),
                                            decision.newJoinStrategy().get())
                                    : subPlanReoptimizer.reoptimize(plan.planFragment());
                    if (reoptimized != plan.planFragment()) {
                        LOG.info("event=aqe_reopt_applied stageId={} upstreamStageId={}"
                                + " newJoinStrategy={}",
                                plan.stageId(), upstreamId,
                                decision.newJoinStrategy().map(Enum::name).orElse("none"));
                        plan = plan.withPlanFragment(reoptimized);
                    }
                } else {
                    LOG.warn("event=aqe_reopt_detected_no_reoptimizer stageId={}"
                            + " upstreamStageId={} newJoinStrategy={}",
                            plan.stageId(), upstreamId,
                            decision.newJoinStrategy().map(Enum::name).orElse("none"));
                }
            }
            // F-COORD-21: previously a `break;` here exited after the first
            // upstream, silently dropping coalesce/parallelism/skew decisions
            // from every other upstream. For JOIN stages with two upstreams
            // (build + probe) only the build side was applied, leaving the
            // probe side without skew/coalesce mitigation.
        }
        return plan;
    }

    private List<SplitFetcher.SplitWithPaths> fetchSplits(
            StageNode plan, StageDAG dag, String queryId) {
        java.util.Map<String, Long> overrides = java.util.Map.of();
        DAGQueryContext ctx = queryContextIndex.get(queryId);
        if (ctx != null && !ctx.snapshotOverrides().isEmpty()) {
            overrides = ctx.snapshotOverrides();
        }
        // For OVERWRITE mode (Copy-on-Write DELETE) and UPDATE mode (CoW UPDATE),
        // the native worker reads surviving/unchanged rows from ALL files. The Calcite
        // plan still contains the original WHERE filter as a constraint which would
        // incorrectly prune files that don't match the condition but contain rows
        // that must be preserved. Clear constraints to ensure all data files are included.
        boolean isOverwrite = ctx != null && ctx.writeConfig() != null
                && ("OVERWRITE".equals(ctx.writeConfig().writeMode())
                    || "UPDATE".equals(ctx.writeConfig().writeMode()));
        if (isOverwrite) {
            return fetchSplitsWithoutConstraints(plan, dag, overrides);
        }
        if (indexAwareSplitFetcher != null) {
            return ExchangeSourceBuilder.fetchSplitsWithIndexFiltering(
                    plan, dag, indexAwareSplitFetcher, overrides, queryId);
        }
        return ExchangeSourceBuilder.fetchSplitsWithFilePaths(
                plan, dag, catalogRegistry, overrides);
    }

    private List<SplitFetcher.SplitWithPaths> fetchSplitsWithoutConstraints(
            StageNode plan, StageDAG dag, java.util.Map<String, Long> overrides) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(plan.stageId(), List.of());
        if (!upstreamIds.isEmpty()) {
            return List.of();
        }
        var fragment = plan.planFragment();
        List<SplitSourceDescriptor> sources = fragment.splitSources().stream()
                .map(s -> new SplitSourceDescriptor(
                        s.catalogName(), s.schemaName(), s.tableName(),
                        Constraint.EMPTY, s.tableColumns()))
                .toList();
        List<String> outputColumns = fragment.queryOutputColumns();
        return SplitFetcher.fetchSplitsWithFilePaths(
                sources, catalogRegistry, overrides, outputColumns);
    }

    /**
     * Registers a dispatched task in the task-to-stage registry.
     * Called by {@link LeafStageDispatcher} during result collection.
     * Replays any early terminal event that arrived before registration.
     */
    private void registerTask(String queryId, String taskId, Stage stage) {
        queryTaskIndex.computeIfAbsent(queryId, k -> ConcurrentHashMap.newKeySet())
                .add(taskId);
        var replay = new EarlyTerminalEvent[1];
        taskSlots.compute(taskId, (key, existing) -> {
            if (existing != null && existing.stage() == null) {
                // Early terminal arrived first — capture for replay after compute.
                replay[0] = existing.earlyEvent();
                // Mark slot as replayed to prevent double-routing by concurrent onTaskTerminal.
                return new TaskSlot(stage, null, true);
            }
            return new TaskSlot(stage, null, false);
        });
        LOG.info("event=task_registered queryId={} taskId={} stageState={}",
                queryId, taskId, stage.stateMachine().getState());
        if (replay[0] != null) {
            LOG.info("event=early_terminal_replayed queryId={} taskId={} state={}",
                    queryId, taskId, replay[0].state());
            // Remove slot after replay to prevent a second routing from concurrent onTaskTerminal.
            taskSlots.remove(taskId);
            routeTerminalToStage(stage, taskId, queryId, replay[0].state(), replay[0].error());
        }
    }

    /**
     * Returns true if all stages in the DAG for the given query have finished.
     * Used by TaskStatusReceiver to avoid premature query finalization when
     * downstream stages are still being dispatched after an upstream completes.
     * Returns true when no DAG is tracked (single-stage or non-DAG queries).
     */
    public boolean isQueryComplete(String queryId) {
        StageDAG dag = queryDagIndex.get(queryId);
        if (dag == null) {
            return true;
        }
        return DAGCompletionEvaluator.allStagesFinished(dag);
    }

    /**
     * Receives terminal task state from {@code TaskStatusReceiver} and routes
     * it to the owning Stage. Tasks not dispatched through this engine
     * (e.g. local-dispatch constants) are silently ignored.
     */
    public void onTaskTerminal(String queryId, String taskId,
                               String state, ErrorDetail error) {
        var route = new Stage[1];
        taskSlots.compute(taskId, (key, existing) -> {
            if (existing != null && existing.stage() != null) {
                // Skip routing if slot was already replayed (prevents double-routing).
                if (existing.replayed()) {
                    return existing;
                }
                // Stage already registered — capture for routing after compute.
                route[0] = existing.stage();
                return existing;
            }
            // Stage not yet registered — buffer the terminal event.
            return new TaskSlot(null, new EarlyTerminalEvent(queryId, state, error), false);
        });
        if (route[0] != null) {
            routeTerminalToStage(route[0], taskId, queryId, state, error);
        } else {
            LOG.info("event=early_terminal_buffered queryId={} taskId={} state={}",
                    queryId, taskId, state);
        }
    }

    private void routeTerminalToStage(Stage stage, String taskId,
                                      String queryId, String state, ErrorDetail error) {
        if ("FINISHED".equals(state)) {
            stage.onTaskFinished(taskId);
        } else if ("FAILED".equals(state)) {
            if (taskFaultHandler.isShuffleFetchFailure(error)) {
                LOG.info("event=shuffle_fetch_failure_detected queryId={}"
                        + " taskId={} stageId={}",
                        queryId, taskId, stage.stageId());
            }
            stage.onTaskFailed(taskId, error);
        } else if ("CANCELLED".equals(state)) {
            // Treat cancelled tasks as failed for stage completion tracking.
            // Without this, a cancelled task (e.g. speculative execution loser)
            // that was registered via addTaskId would never be counted in
            // allTasksReported(), causing the stage to hang in RUNNING forever.
            stage.onTaskFailed(taskId, error != null ? error
                    : new ErrorDetail("TASK_CANCELLED", "TaskCancelled",
                            "Task was cancelled"));
        }
        LOG.debug("event=task_terminal_routed queryId={} taskId={} state={}"
                + " stageId={}", queryId, taskId, state, stage.stageId());
    }

    /**
     * Removes all registry entries for the given query to prevent memory leaks.
     * Called on query completion and query failure, and as a safety net from
     * {@code QueryDispatcher.cleanupQuery} for queries that may not have
     * triggered normal completion/failure paths.
     */
    public void clearQuery(String queryId) {
        queryContextIndex.remove(queryId);
        queryDagIndex.remove(queryId);
        taskFaultHandler.clearQuery(queryId);
        Set<String> taskIds = queryTaskIndex.remove(queryId);
        if (taskIds != null) {
            for (String taskId : taskIds) {
                taskSlots.remove(taskId);
            }
        }
    }

    private void emitStageLineage(String queryId, StageNode stage, boolean start) {
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

    public record StageDispatchRecord(
            int stageId, DispatchResult result,
            int workerHttpPort, int workerTransportPort) {}

    record PartitionDispatch(
            CompletableFuture<DispatchResult> future,
            int workerHttpPort, int workerTransportPort,
            String workerId,
            org.pangrova.protocol.rest.task.SubmitTaskRequest request) {

        /** Backward-compatible constructor without request. */
        PartitionDispatch(CompletableFuture<DispatchResult> future,
                          int workerHttpPort, int workerTransportPort,
                          String workerId) {
            this(future, workerHttpPort, workerTransportPort, workerId, null);
        }
    }

    /** Buffered terminal event for tasks that complete before registration. */
    record EarlyTerminalEvent(String queryId, String state, ErrorDetail error) {}

    /**
     * Unified slot for task-to-stage mapping and early terminal buffering.
     * Accessed exclusively via {@link ConcurrentHashMap#compute} on
     * {@link #taskSlots} to guarantee atomic check-and-act between
     * {@link #registerTask} and {@link #onTaskTerminal}.
     *
     * @param stage      the owning Stage, or null if terminal arrived first
     * @param earlyEvent the buffered terminal event, or null if stage registered first
     */
    record TaskSlot(Stage stage, EarlyTerminalEvent earlyEvent, boolean replayed) {}
}
