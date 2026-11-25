package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.write.WriteAuthorityResolver;

import org.pangrova.planner.analysis.AuthorizedRelation;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.query.lifecycle.QueryDispatchOutcome;

import org.pangrova.coordinator.scheduler.placement.DefaultWorkerSelector;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.persistence.query.api.lifecycle.ShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.observability.sdk.tracing.QueryTracer;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.shuffle.api.ShuffleCleanupRecovery;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
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
public class QueryDispatcher implements org.pangrova.query.lifecycle.QueryExecutionPort {

    private static final Logger LOG = LoggerFactory.getLogger(QueryDispatcher.class);
    private static final long DEFAULT_DAG_DISPATCH_TIMEOUT_MS = 300_000L;

    private final QueryPipelineExecutor pipelineExecutor;
    private final QuerySchedulingEngine schedulingEngine;
    private final QueryOwnershipTracker ownershipTracker;
    private final QueryLineageCapture lineageCapture;
    private final CoordinatorTaskTracker taskTracker;
    private final TaskDispatcher dispatcher;
    private final DefaultWorkerSelector workerSelector;
    private final WriteAuthorityResolver writeAuthorityResolver;
    private final ShuffleLifecycleManager shuffleLifecycleManager;

    public QueryDispatcher(QueryDispatcherContext deps,
                           StatementDispatcher preBuiltDispatcher) {
        this(deps, preBuiltDispatcher, null);
    }

    /** Assembles the shared query execution pipeline with optional tracing. */
    public QueryDispatcher(QueryDispatcherContext deps,
                           StatementDispatcher preBuiltDispatcher,
                           TracingProvider tracingProvider) {
        this(deps, preBuiltDispatcher, tracingProvider, null);
    }

    /**
     * Fixes current write authority for this dispatcher lifetime. A missing resolver
     * leaves reads available but rejects every write before connector allocation.
     */
    public QueryDispatcher(QueryDispatcherContext deps,
                           StatementDispatcher preBuiltDispatcher,
                           TracingProvider tracingProvider,
                           WriteAuthorityResolver writeAuthorityResolver) {
        checkNotNull(deps, "deps must not be null");

        this.lineageCapture = new QueryLineageCapture();
        this.taskTracker = deps.taskTracker();
        this.dispatcher = deps.dispatcher();
        this.shuffleLifecycleManager = deps.shuffleLifecycleManager();

        var securityApplier = createSecurityApplier(deps);
        var queryTracer = tracingProvider != null
                ? new QueryTracer(tracingProvider) : null;
        this.workerSelector = createWorkerSelector(deps);
        this.writeAuthorityResolver = writeAuthorityResolver;
        var dagEngine = createDagEngine(deps);
        long dagDispatchTimeoutMs = deps.taskTimeoutSeconds() > 0
                ? deps.taskTimeoutSeconds() * 1000L : DEFAULT_DAG_DISPATCH_TIMEOUT_MS;

        this.schedulingEngine = new QuerySchedulingEngine(
                new QuerySchedulingEngine.SchedulingDependencies(
                        dagEngine, this.workerSelector,
                        deps.workerManager(), deps.taskTracker(),
                        deps.catalogRegistry(),
                        deps.indexAwareSplitFetcher(),
                        dagDispatchTimeoutMs), writeAuthorityResolver);

        this.pipelineExecutor = new QueryPipelineExecutor(
                securityApplier, preBuiltDispatcher,
                queryTracer, lineageCapture,
                schedulingEngine,
                deps.catalogRegistry());

        this.ownershipTracker = new QueryOwnershipTracker(
                deps.taskTracker(), deps.workerManager(),
                deps.catalogRegistry(), dagEngine.cancelRetrier());
        this.ownershipTracker.initFromContext(
                deps.haService(), deps.coordinatorId());
    }

    // ── Public API ──

    /** Synchronous callers await the same execution used by asynchronous submission. */
    public DispatchResult execute(QueryStateMachine sm, String sql, QuerySession session) {
        return DAGCompletionEvaluator.awaitResult(executeAsync(sm, sql, session), session.queryId(),
                () -> cancelDispatchedTasks(session.queryId()));
    }

    /** Retains the admitted operation through planning and final execution callbacks. */
    public CompletionStage<DispatchResult> executeAsync(
            QueryStateMachine sm, String sql, QuerySession session) {
        checkNotNull(sm, "state machine must not be null");
        checkNotNull(sql, "sql must not be null");
        checkNotNull(session, "session must not be null");
        var authorizedSession = bindConnectorAuthority(sm, session);
        return trackExecution(session.queryId(), () -> pipelineExecutor.doExecute(sm, sql, authorizedSession));
    }

    private CompletionStage<DispatchResult> trackExecution(String queryId,
            Supplier<CompletionStage<DispatchResult>> start) {
        var operation = taskTracker.enterQueryOperation(queryId);
        try {
            return start.get().whenComplete((result, failure) ->
                    ResourceCloser.closeAll(failure, operation::close));
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, operation::close);
            throw failure;
        }
    }

    /** Compiles a protected read relation using this dispatcher's current query authority. */
    public AuthorizedRelation compileAuthorizedRelation(String sql, QuerySession session) {
        checkNotNull(sql, "sql must not be null");
        return pipelineExecutor.compileAuthorizedRelation(sql, session);
    }

    /** Compiles and schedules a parsed DML statement under the admitted authority. */
    public CompletionStage<DispatchResult> startDmlStatement(
            QueryStateMachine sm, org.pangrova.parser.api.ast.Statement stmt, QuerySession session) {
        var authorizedSession = bindConnectorAuthority(sm, session);
        return trackExecution(session.queryId(), () -> pipelineExecutor.startDmlStatement(sm, stmt, authorizedSession));
    }

    public QueryLineageInfo removeLineageInfo(String queryId) {
        return lineageCapture.removeLineageInfo(queryId);
    }

    private QuerySession bindConnectorAuthority(QueryStateMachine state,
            QuerySession session) {
        var authority = bindConnectorAuthority(state, session.queryId());
        return session.withQueryAuthority(authority);
    }

    /** Registers the exact public state-bound lifetime before planning can perform connector I/O. */
    public org.pangrova.connector.api.metadata.ConnectorMetadataScope bindConnectorAuthority(
            QueryStateMachine state, String queryId) {
        var authority = state.bindConnectorAuthority(taskTracker.openQueryIssuance(queryId));
        ownershipTracker.registerConnectorAuthority(authority);
        return authority;
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

    @Override
    public void releaseQueryOwnership(String queryId) {
        ownershipTracker.releaseQueryOwnership(queryId);
    }

    public org.pangrova.connector.api.query.ConnectorQueryAuthority openQueryIssuance(String queryId) {
        return taskTracker.openQueryIssuance(queryId);
    }

    public org.pangrova.query.api.lifecycle.QueryOperation enterQueryOperation(String queryId) {
        return taskTracker.enterQueryOperation(queryId);
    }

    public org.pangrova.query.api.lifecycle.QueryOperation enterQueryMetadataOperation(String queryId) {
        return taskTracker.enterQueryMetadataOperation(queryId);
    }

    public void markRecoveredIssuanceUnresolved(String queryId) {
        taskTracker.markRecoveredIssuanceUnresolved(queryId);
    }

    public void retireQueryIssuance(String queryId) { taskTracker.retireQueryIssuance(queryId); }

    public void addQueryDrainProgressListener(java.util.function.Consumer<String> listener) {
        taskTracker.addQueryDrainProgressListener(listener);
    }

    @Override
    public boolean tryCleanupQuery(String queryId) {
        if (!ownershipTracker.tryCleanupQuery(queryId)) return false;
        schedulingEngine.dagEngine().clearQuery(queryId);
        return true;
    }

    /** Cleans the actual local creation scope without touching foreign execution ownership. */
    @Override
    public boolean tryCleanupUnadmittedQuery(String queryId) {
        if (!ownershipTracker.tryCleanupLocalQuery(queryId)) return false;
        schedulingEngine.dagEngine().clearQuery(queryId);
        return true;
    }

    /** Wires the one durable remote Shuffle cleanup path. */
    public void setShuffleCleanupIntents(ShuffleCleanupIntentStore intents) {
        ownershipTracker.setShuffleCleanup(intents, requireShuffleCleanupRecovery());
    }

    /** Retries a persisted terminal Shuffle cleanup after Coordinator restart. */
    public void retryPendingShuffleCleanup(String queryId) {
        requireShuffleCleanupRecovery().recoverCleanupShuffle(queryId);
    }

    private ShuffleCleanupRecovery requireShuffleCleanupRecovery() {
        if (shuffleLifecycleManager instanceof ShuffleCleanupRecovery recovery) {
            return recovery;
        }
        throw new IllegalStateException("Shuffle provider does not support durable cleanup recovery");
    }

    @Override
    public void cancelDispatchedTasks(String queryId) {
        org.pangrova.common.lifecycle.ResourceCloser.closeAll(null,
                () -> ownershipTracker.cancelDispatchedTasks(queryId),
                () -> schedulingEngine.dagEngine().cancelLiveDispatch(queryId));
    }

    @Override
    public QueryDispatchOutcome dispatch(QueryStateMachine sm, String sql, QuerySession session) {
        execute(sm, sql, session);
        return dispatchOutcome(sm);
    }

    @Override
    public CompletionStage<QueryDispatchOutcome> dispatchAsync(
            QueryStateMachine sm, String sql, QuerySession session) {
        return executeAsync(sm, sql, session).thenApply(ignored -> dispatchOutcome(sm));
    }

    private static QueryDispatchOutcome dispatchOutcome(QueryStateMachine state) {
        var status = state.status();
        return new QueryDispatchOutcome(status.disposition(), status.endpoint());
    }

    @Override
    public void retryPendingCancels() {
        cancelRetrier().retryPendingCancels();
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

    /** Returns the exact Worker placement strategy used by dispatched tasks. */
    public WorkerSelector workerSelector() { return workerSelector; }

    /** Returns the dispatcher used for task transport. */
    public TaskDispatcher taskDispatcher() { return dispatcher; }

    /** Returns the write authority resolver fixed for this dispatcher lifetime. */
    public WriteAuthorityResolver writeAuthorityResolver() { return writeAuthorityResolver; }

    private static DefaultWorkerSelector createWorkerSelector(
            QueryDispatcherContext deps) {
        return new DefaultWorkerSelector(
                deps.workerManager(), deps.taskScheduler(), deps.cacheAffinityAssigner(),
                deps.spillBudget() != null
                        ? deps.spillBudget() : new SpillBudget(0),
                deps.embeddingModelRegistry());
    }

    private static DAGDispatchEngine createDagEngine(
            QueryDispatcherContext deps) {
        return new DAGDispatchEngine(new DAGDispatchContext(
                deps.dispatcher(), deps.taskTracker(),
                deps.lineageEmitter(), deps.catalogRegistry(),
                deps.taskTimeoutSeconds(), deps.adaptiveQueryExecutor(),
                deps.adaptiveQueryConfig(),
                deps.speculativeStageHandler(),
                deps.workerManager().requiresLocalShuffle() ? PipelineConfig.LOCAL : PipelineConfig.ENABLED,
                deps.shuffleLifecycleManager(),
                deps.resilienceRegistry(), deps.stateJournal()),
                buildStageHandlers(deps),
                deps.indexAwareSplitFetcher(), deps.artifactBindingResolver(), deps.generatedValueReadResolver());
    }

    private static List<org.pangrova.coordinator.query.execution.dispatch
            .dag.handler.StageLifecycleHandler> buildStageHandlers(
            QueryDispatcherContext deps) {
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
                            deps.skewDetector(), deps.adaptiveQueryConfig(),
                            deps.adaptiveQueryExecutor())));
        }
        if (deps.resilienceRegistry() != null && !deps.workerManager().requiresLocalShuffle()) {
            handlers.add(new org.pangrova.coordinator.query.execution
                    .dispatch.dag.handler.RetryStageHandler(
                    deps.resilienceRegistry()));
        }
        return List.copyOf(handlers);
    }

}
