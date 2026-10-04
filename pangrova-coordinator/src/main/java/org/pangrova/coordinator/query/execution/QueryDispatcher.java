package org.pangrova.coordinator.query.execution;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.governance.lineage.DatasetRef;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.observability.tracing.QueryTracer;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Orchestrates the query execution pipeline via a phase-based architecture.
 * Pipeline: StatementDispatch → Compilation → CacheLookup → DistributedDispatch.
 *
 * <p>This class is a thin facade that delegates to:
 * <ul>
 *   <li>{@link QueryPipelineExecutor} — pipeline execution and DML compilation</li>
 *   <li>{@link QuerySchedulingEngine} — dispatch to workers and DAG execution</li>
 * </ul>
 *
 * <p>Ownership tracking and query cleanup are delegated to
 * {@link QueryOwnershipTracker}.</p>
 *
 * <p>Concurrency strategy: this class is shared across concurrent query
 * submissions. All mutable state uses thread-safe constructs.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public class QueryDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(QueryDispatcher.class);
    private static final long DEFAULT_DAG_DISPATCH_TIMEOUT_MS = 300_000L;

    private final QueryPipelineExecutor pipelineExecutor;
    private final QuerySchedulingEngine schedulingEngine;
    private final QueryOwnershipTracker ownershipTracker;
    private final QueryLineageCapture lineageCapture;
    private final LineageEventEmitter lineageEmitter;
    private final CoordinatorTaskTracker taskTracker;
    private final TaskDispatcher dispatcher;
    private final ShuffleLifecycleManager shuffleLifecycleManager;

    public QueryDispatcher(QueryDispatcherContext deps,
                           StatementDispatcher preBuiltDispatcher) {
        this(deps, preBuiltDispatcher, null);
    }

    /** Assembles the shared query execution pipeline with optional tracing. */
    public QueryDispatcher(QueryDispatcherContext deps,
                           StatementDispatcher preBuiltDispatcher,
                           TracingProvider tracingProvider) {
        checkNotNull(deps, "deps must not be null");

        this.lineageCapture = new QueryLineageCapture();
        this.lineageEmitter = deps.lineageEmitter();
        this.taskTracker = deps.taskTracker();
        this.dispatcher = deps.dispatcher();
        this.shuffleLifecycleManager = deps.shuffleLifecycleManager();

        var securityApplier = createSecurityApplier(deps);
        var queryTracer = tracingProvider != null
                ? new QueryTracer(tracingProvider) : null;
        var splitMgr = resolveSplitAssignmentManager(deps);
        var workerSelector = createWorkerSelector(deps, splitMgr);
        var rfHandler = createRuntimeFilterHandler(deps);
        var dagEngine = createDagEngine(deps, splitMgr, rfHandler);
        long dagDispatchTimeoutMs = deps.taskTimeoutSeconds() > 0
                ? deps.taskTimeoutSeconds() * 1000L : DEFAULT_DAG_DISPATCH_TIMEOUT_MS;

        this.schedulingEngine = new QuerySchedulingEngine(
                new QuerySchedulingEngine.SchedulingDependencies(
                        dagEngine, workerSelector,
                        deps.workerManager(), deps.taskTracker(),
                        deps.catalogRegistry(),
                        deps.indexAwareSplitFetcher(), rfHandler,
                        dagDispatchTimeoutMs));

        this.pipelineExecutor = new QueryPipelineExecutor(
                securityApplier, preBuiltDispatcher,
                queryTracer, lineageCapture,
                schedulingEngine,
                deps.catalogRegistry());

        var cancelRetrier = new TaskCancelRetrier(
                deps.dispatcher(), deps.taskTracker());
        this.ownershipTracker = new QueryOwnershipTracker(
                deps.taskTracker(), deps.workerManager(),
                deps.catalogRegistry(), splitMgr, cancelRetrier);
        this.ownershipTracker.initFromContext(
                deps.haService(), deps.coordinatorId());
        this.schedulingEngine.setConnectorRegistrar(ownershipTracker::registerConnectorMetadata);
    }

    // ── Public API ──

    /** Unified execution entry point. All paths return DispatchResult. */
    public DispatchResult execute(QueryStateMachine sm, String sql,
                                  CoordinatorSessionContext session) {
        checkNotNull(sm, "state machine must not be null");
        checkNotNull(sql, "sql must not be null");
        checkNotNull(session, "session must not be null");
        taskTracker.openQueryIssuance(session.queryId());
        try (var operation = taskTracker.enterQueryOperation(session.queryId())) {
            DispatchResult result = pipelineExecutor.doExecute(
                    sm, sql, session);
            emitQueryStartedLineage(session.queryId(), sql);
            return result;
        } catch (Exception e) {
            LOG.error("event=execute_crash queryId={} error={}",
                    session.queryId(), e.getMessage(), e);
            // QueryManager owns the terminal transition and its durable journal,
            // active-count and admission cleanup. This layer only releases the
            // dispatch ownership it acquired before propagating the failure.
            try { ownershipTracker.cancelDispatchedTasks(session.queryId());
                ownershipTracker.cleanupQuery(session.queryId());
            } catch (RuntimeException | Error cleanup) { if (cleanup != e) e.addSuppressed(cleanup); }
            throw wrapIfNeeded(e, session.queryId());
        }
    }

    public DispatchResult executeDmlSql(QueryStateMachine sm, String sql,
                                        CoordinatorSessionContext session) {
        return executeDmlSql(sm, sql, session, null);
    }

    /** Compiles a protected read relation using this dispatcher's current query authority. */
    public AuthorizedRelation compileAuthorizedRelation(String sql, CoordinatorSessionContext session) {
        checkNotNull(sql, "sql must not be null");
        return pipelineExecutor.compileAuthorizedRelation(sql, session);
    }

    public DispatchResult executeDmlSql(QueryStateMachine sm, String sql,
                                        CoordinatorSessionContext session,
                                        String writeModeOverride) {
        taskTracker.openQueryIssuance(session.queryId());
        try (var operation = taskTracker.enterQueryOperation(session.queryId())) {
            return pipelineExecutor.executeDmlSql(sm, sql, session, writeModeOverride);
        }
    }

    public DispatchResult executeDmlStatement(
            QueryStateMachine sm,
            org.pangrova.parser.ast.Statement stmt,
            CoordinatorSessionContext session) {
        taskTracker.openQueryIssuance(session.queryId());
        try (var operation = taskTracker.enterQueryOperation(session.queryId())) {
            return pipelineExecutor.executeDmlStatement(sm, stmt, session);
        }
    }

    public QueryLineageInfo removeLineageInfo(String queryId) {
        return lineageCapture.removeLineageInfo(queryId);
    }

    public List<String> removeColumnNames(String queryId) {
        return lineageCapture.removeColumnNames(queryId);
    }

    public void setOwnershipTracking(
            org.pangrova.coordinator.coordination
                    .HighAvailabilityCoordinationService haService,
            String coordinatorId) {
        ownershipTracker.setOwnershipTracking(haService, coordinatorId);
    }

    public void setWriteCommitTracker(
            org.pangrova.coordinator.query.task.write
                    .NativeWriteCommitTracker tracker) {
        schedulingEngine.setWriteCommitTracker(tracker);
    }

    /** Binds the production generated-write authority before query submission starts. */
    public void setGeneratedWriteAuthoritySupplier(GeneratedWriteAuthoritySupplier supplier) {
        schedulingEngine.setGeneratedWriteAuthoritySupplier(supplier);
    }

    /**
     * Injects self-intelligence components into the query pipeline for
     * workload analysis.
     */
    public void setSelfIntelligence(
            org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder) {
        if (patternRecorder != null) {
            pipelineExecutor.setPatternRecorder(patternRecorder);
        }
    }

    public DAGDispatchEngine dagEngine() {
        return schedulingEngine.dagEngine();
    }

    public boolean acquireQueryOwnership(String queryId) {
        return ownershipTracker.acquireQueryOwnership(queryId);
    }

    public void openQueryIssuance(String queryId) { taskTracker.openQueryIssuance(queryId); }

    public org.pangrova.coordinator.task.QueryIssuanceOperation enterQueryOperation(String queryId) { return taskTracker.enterQueryOperation(queryId); }

    public org.pangrova.coordinator.task.QueryIssuanceOperation enterQueryMetadataOperation(String queryId) {
        return taskTracker.enterQueryMetadataOperation(queryId);
    }

    public void markRecoveredIssuanceUnresolved(String queryId) { taskTracker.markRecoveredIssuanceUnresolved(queryId); }

    public void retireQueryIssuance(String queryId) { taskTracker.retireQueryIssuance(queryId); }

    public void addExecutionDrainListener(java.util.function.Consumer<String> listener) {
        taskTracker.addExecutionDrainListener(report -> listener.accept(report.getAttempt().getQueryOrJobId()));
    }

    public void cleanupQuery(String queryId) {
        ownershipTracker.cleanupQuery(queryId);
        schedulingEngine.dagEngine().clearQuery(queryId);
    }

    /** Wires the one durable remote Shuffle cleanup path. */
    public void setShuffleCleanupIntents(JdbcShuffleCleanupIntentStore intents) {
        if (shuffleLifecycleManager == null) {
            throw new IllegalStateException("remote Shuffle lifecycle is not configured");
        }
        ownershipTracker.setShuffleCleanup(intents, shuffleLifecycleManager);
    }

    /** Retries a persisted terminal Shuffle cleanup after Coordinator restart. */
    public void retryPendingShuffleCleanup(String queryId) {
        if (!(shuffleLifecycleManager
                instanceof org.pangrova.coordinator.query.execution.dispatch.shuffle
                        .CoordinatorShuffleLifecycleManager remote)) {
            throw new IllegalStateException("remote Shuffle lifecycle is not configured");
        }
        remote.recoverCleanupShuffle(queryId);
    }

    public void cancelDispatchedTasks(String queryId) {
        ownershipTracker.cancelDispatchedTasks(queryId);
    }

    public TaskCancelRetrier cancelRetrier() {
        return ownershipTracker.cancelRetrier();
    }

    /** Initiates dynamic filter injection for a running query. */
    public void injectDynamicFilter(String queryId, String taskId,
                                    String planNodeId, byte[] filterProto) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(taskId, "taskId must not be null");
        checkNotNull(planNodeId, "planNodeId must not be null");
        checkNotNull(filterProto, "filterProto must not be null");
        String workerId = taskTracker.getWorkerForTask(taskId);
        if (workerId == null) {
            LOG.warn("event=dynamic_filter_skip queryId={} taskId={} "
                    + "reason=worker_not_found", queryId, taskId);
            return;
        }
        LOG.info("event=dynamic_filter_inject queryId={} taskId={} "
                + "planNodeId={}", queryId, taskId, planNodeId);
        dispatcher.injectDynamicFilter(
                taskId, planNodeId, filterProto, workerId);
    }

    // ── Construction factory methods ──

    private static SecurityPolicyApplier createSecurityApplier(
            QueryDispatcherContext deps) {
        return new SecurityPolicyApplier(
                deps.compiler(), deps.accessControlEnforcer(),
                deps.columnMaskApplier(), deps.rowFilterApplier());
    }

    private static SplitAssignmentManager resolveSplitAssignmentManager(
            QueryDispatcherContext deps) {
        return deps.splitAssignmentManager() != null
                ? deps.splitAssignmentManager()
                : new SplitAssignmentManager();
    }

    private static DefaultWorkerSelector createWorkerSelector(
            QueryDispatcherContext deps, SplitAssignmentManager splitMgr) {
        return new DefaultWorkerSelector(
                deps.workerManager(), deps.taskScheduler(), splitMgr,
                deps.spillBudget() != null
                        ? deps.spillBudget() : new SpillBudget(0, ""),
                deps.embeddingModelRegistry());
    }

    private static org.pangrova.coordinator.query.execution.dispatch.dag
            .handler.RuntimeFilterStageHandler createRuntimeFilterHandler(
            QueryDispatcherContext deps) {
        if (deps.runtimeFilterRegistry() == null) {
            return null;
        }
        return new org.pangrova.coordinator.query.execution.dispatch.dag
                .handler.RuntimeFilterStageHandler(
                new org.pangrova.planner.runtimefilter
                        .RuntimeFilterGenerator(),
                deps.runtimeFilterRegistry(),
                deps.dispatcher(),
                deps.taskTracker(),
                new org.pangrova.coordinator.query.execution.dispatch.dag
                        .handler.ShuffleOutputCollector());
    }

    private static DAGDispatchEngine createDagEngine(
            QueryDispatcherContext deps, SplitAssignmentManager splitMgr,
            org.pangrova.coordinator.query.execution.dispatch.dag
                    .handler.RuntimeFilterStageHandler rfHandler) {
        return new DAGDispatchEngine(new DAGDispatchContext(
                deps.dispatcher(), deps.taskTracker(),
                deps.lineageEmitter(), deps.catalogRegistry(),
                deps.taskTimeoutSeconds(), deps.adaptiveQueryExecutor(),
                deps.runtimeFilterRegistry(), deps.adaptiveQueryConfig(),
                deps.speculativeStageHandler(), PipelineConfig.ENABLED,
                deps.shuffleLifecycleManager(), splitMgr,
                deps.resilienceRegistry(), deps.stateJournal()),
                buildStageHandlers(deps, rfHandler),
                deps.indexAwareSplitFetcher());
    }

    private static List<org.pangrova.coordinator.query.execution.dispatch
            .dag.handler.StageLifecycleHandler> buildStageHandlers(
            QueryDispatcherContext deps,
            org.pangrova.coordinator.query.execution.dispatch.dag
                    .handler.RuntimeFilterStageHandler rfHandler) {
        var handlers = new ArrayList<org.pangrova.coordinator.query.execution
                .dispatch.dag.handler.StageLifecycleHandler>();
        if (deps.shuffleLifecycleManager() != null) {
            handlers.add(new org.pangrova.coordinator.query.execution
                    .dispatch.dag.handler.ShuffleStageHandler(
                    deps.shuffleLifecycleManager()));
        }
        if (deps.adaptiveQueryConfig() != null
                && deps.adaptiveQueryExecutor() != null) {
            handlers.add(new org.pangrova.coordinator.query.execution
                    .dispatch.dag.handler.AdaptiveQueryStageHandler(
                    new org.pangrova.coordinator.query.execution
                            .dispatch.adaptive.AdaptiveQueryEvaluator(
                            deps.adaptiveQueryConfig(),
                            deps.adaptiveQueryExecutor())));
        }
        if (deps.resilienceRegistry() != null) {
            handlers.add(new org.pangrova.coordinator.query.execution
                    .dispatch.dag.handler.RetryStageHandler(
                    deps.resilienceRegistry()));
        }
        if (rfHandler != null) {
            handlers.add(rfHandler);
        }
        return List.copyOf(handlers);
    }

    // ── Internal helpers ──

    private void emitQueryStartedLineage(String queryId, String sql) {
        if (lineageEmitter == null) {
            return;
        }
        try {
            QueryLineageInfo info = lineageCapture.peekLineageInfo(queryId);
            List<DatasetRef> inputs = info != null
                    ? toDatasetRefs(info.sourceTables()) : List.of();
            List<DatasetRef> outputs = info != null
                    ? toDatasetRefs(info.targetTables()) : List.of();
            lineageEmitter.emitStartEvent(queryId, sql, inputs, outputs);
        } catch (Exception e) {
            LOG.debug("event=lineage_query_started_failed queryId={} "
                    + "error={}", queryId, e.getMessage());
        }
    }

    private static List<DatasetRef> toDatasetRefs(List<String> tableNames) {
        return tableNames.stream()
                .map(name -> new DatasetRef("pangrova", name, List.of()))
                .toList();
    }

    private static RuntimeException wrapIfNeeded(Exception e,
                                                  String queryId) {
        if (e instanceof PangrovaException) {
            return (PangrovaException) e;
        }
        if (e instanceof RuntimeException re) {
            return re;
        }
        return new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                "Query execution failed [queryId=" + queryId + "]", e);
    }
}
