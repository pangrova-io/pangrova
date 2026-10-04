package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TracingProvider;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.observability.logging.LogContext;
import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.coordinator.query.tenant.TenantQueryGuard;

@ThreadSafe
public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    private static final String DRAIN_REJECTION_MESSAGE =
            "Coordinator is draining, not accepting new queries";

    private final ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
    private final QueryTerminalCleanup terminalCleanup;
    private final QueryAdmission admission;
    private final AtomicInteger activeQueryCount = new AtomicInteger(0);

    @javax.annotation.concurrent.GuardedBy("itself")
    private final ReentrantReadWriteLock queryLifecycleLock = new ReentrantReadWriteLock();
    private final java.util.concurrent.locks.ReentrantLock drainSignalLock =
            new java.util.concurrent.locks.ReentrantLock();
    private final Condition allQueriesDrained = drainSignalLock.newCondition();

    private final QueryDispatcher queryDispatcher;
    private final TracingProvider tracingProvider;
    private final TenantQueryGuard tenantGuard;
    private final QueryCleanupScheduler cleanupScheduler;
    private final QueryAuditDispatcher auditBridge;
    private final LongCounter querySubmitCounter;
    private final QueryTerminalEvents terminalEvents;
    private volatile boolean draining;
    private final QueryJournalLifecycle journalLifecycle;
    private final AsyncSubmitConfig asyncSubmitConfig;
    private final ExecutorService asyncExecutor;
    private volatile java.util.function.Consumer<String> shuffleCleanupPending = ignored -> { };

    public QueryManager(QueryManagerContext deps) {
        this(deps, System::nanoTime);
    }

    QueryManager(QueryManagerContext deps, LongSupplier nanoClock) {
        this.queryDispatcher = deps.queryDispatcher();
        this.tracingProvider = deps.tracingProvider();
        this.tenantGuard = QueryAdmission.tenantGuard(deps, queries.values());
        this.terminalCleanup = new QueryTerminalCleanup(queryDispatcher, this::appendTerminalJournal,
                this::emitTerminalEvents,
                this::releasePhysicalQuery, java.util.Objects.requireNonNull(nanoClock, "nanoClock"));
        this.queryDispatcher.addExecutionDrainListener(terminalCleanup::onExecutionDrain);
        this.cleanupScheduler = new QueryCleanupScheduler(queries,
                terminalCleanup::isPending, this::retryPendingTerminalCompletions);
        this.auditBridge = new QueryAuditDispatcher(
                QueryAuditDispatcher.createAuditEmitter(deps.auditEmitter(), deps.queryAuditService()),
                deps.lineageEmitter(),
                deps.queryDispatcher());
        this.admission = new QueryAdmission(deps.resilienceRegistry(), deps.throttlingService());
        this.journalLifecycle = new QueryJournalLifecycle(deps.stateJournal() != null
                ? deps.stateJournal() : new InMemoryQueryStateJournal());
        this.asyncSubmitConfig = deps.asyncSubmitConfig();
        this.asyncExecutor = deps.asyncExecutor();
        this.querySubmitCounter = createCounter(deps.metricsProvider(),
                "pangrova.coordinator.queries.submitted", "Total queries submitted", "queries");
        this.terminalEvents = new QueryTerminalEvents(auditBridge, admission,
                createCounter(deps.metricsProvider(), "pangrova.coordinator.queries.completed",
                        "Total queries completed", "queries"),
                createCounter(deps.metricsProvider(), "pangrova.coordinator.queries.failed",
                        "Total queries failed", "queries"));
    }

    private static LongCounter createCounter(MetricsProvider provider,
                                              String name, String description, String unit) {
        if (provider == null) {
            return null;
        }
        return provider.createCounter(name, description, unit);
    }

    public void setShuffleCleanupPending(java.util.function.Consumer<String> recorder) {
        shuffleCleanupPending = java.util.Objects.requireNonNull(recorder, "recorder");
        queries.forEach((queryId, sm) -> sm.setBeforeTerminalTransition(
                () -> shuffleCleanupPending.accept(queryId)));
    }

    private void incrementCounter(LongCounter counter) { if (counter != null) counter.add(1); }

    public SubmitQueryResponse submitQuery(String queryId, String sql, CoordinatorSessionContext session) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(sql, "sql must not be null");
        checkNotNull(session, "session must not be null");
        QueryStateMachine stateMachine;
        org.pangrova.coordinator.task.QueryIssuanceOperation submission;
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) {
                throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            }
            stateMachine = resolveOrCreateStateMachine(queryId, session);
            submission = queryDispatcher.enterQueryOperation(queryId);
        } finally {
            queryLifecycleLock.readLock().unlock();
        }
        Throwable submissionFailure = null;
        try {
            admission.checkDegradation();
            Span span = tracingProvider.startSpan("query.submit", SpanKind.SERVER);
            span.setAttribute("query.id", queryId);
            LogContext.set(NativeTraceExporter.currentTraceId(), NativeTraceExporter.currentSpanId());
            try {
                incrementCounter(querySubmitCounter);
                LOG.info("event=query_submitted queryId={}", queryId);
                auditBridge.emitAuditSubmitted(queryId, sql, session);
                return executeQuery(queryId, sql, session, stateMachine);
            } finally {
                LogContext.clear();
                span.end();
            }
        } catch (RuntimeException | Error failure) {
            submissionFailure = failure;
            stateMachine.resolveOwnership(false);
            try {
                failQuery(queryId, new ErrorDetail("QUERY_SUBMISSION_FAILED", failure.getClass().getSimpleName(),
                        String.valueOf(failure.getMessage())), "submission_failed");
            } catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        } finally {
            finishSubmission(submission, queryId, stateMachine, submissionFailure);
        }
    }

    private void finishSubmission(org.pangrova.coordinator.task.QueryIssuanceOperation submission,
            String queryId, QueryStateMachine state, Throwable original) {
        try {
            submission.close();
            if (state.isTerminal()) completeTerminalQuery(queryId, state, "submission_exited");
        } catch (RuntimeException | Error cleanup) {
            if (original == null) throw cleanup;
            if (original != cleanup) original.addSuppressed(cleanup);
        }
    }

    private SubmitQueryResponse executeQuery(String queryId, String sql, CoordinatorSessionContext session,
                                             QueryStateMachine sm) {
        CoordinatorSessionContext enriched;
        try {
            registerJournalListener(queryId, sm);

            enriched = enrichTenant(session, sm);
            admission.checkThrottle(queryId, enriched);

            admission.checkCluster(queryId, enriched.tenantId(), sm);

            acquireOwnershipOrReject(queryId);

            journalLifecycle.queued(queryId, sql, enriched, sm);

            sm.resolveOwnership(true);
        } catch (RuntimeException | Error failure) {
            sm.resolveOwnership(false);
            try {
                failQuery(queryId, new ErrorDetail("QUERY_ADMISSION_FAILED", failure.getClass().getSimpleName(),
                        String.valueOf(failure.getMessage())), "admission_failed");
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }

        if (asyncSubmitConfig != null && asyncSubmitConfig.enabled() && asyncExecutor != null) {
            return executeQueryAsync(queryId, sql, enriched, sm);
        }

        DispatchResult result;
        try {
            result = queryDispatcher.execute(sm, sql, enriched);
        } catch (RuntimeException failure) {
            failQuery(queryId, new ErrorDetail(
                    "INTERNAL_ERROR", failure.getClass().getSimpleName(),
                    failure.getMessage()), "dispatch_failed");
            throw failure;
        }
        applyDispatchResult(queryId, sm, result);
        return buildResponse(queryId, sm, result, queryDispatcher.removeColumnNames(queryId));
    }

    private SubmitQueryResponse executeQueryAsync(String queryId, String sql,
                                                 CoordinatorSessionContext session, QueryStateMachine state) {
        return new QueryAsyncSubmission(queryDispatcher, asyncSubmitConfig, asyncExecutor,
                this::failQuery, this::applyDispatchResult).execute(queryId, sql, session, state);
    }

    private CoordinatorSessionContext enrichTenant(
            CoordinatorSessionContext session, QueryStateMachine sm) {
        CoordinatorSessionContext enriched = enrichWithTenant(session);
        if (enriched.tenantId() != null) {
            sm.setTenantId(enriched.tenantId());
        }
        return enriched;
    }

    private void acquireOwnershipOrReject(String queryId) {
        if (!queryDispatcher.acquireQueryOwnership(queryId)) {
            throw new QueryRejectedException(
                    "Failed to acquire query ownership for queryId=" + queryId);
        }
    }

    private QueryStateMachine resolveOrCreateStateMachine(String queryId, CoordinatorSessionContext session) {
        QueryStateMachine sm = new QueryStateMachine(true, session);
        sm.setTransitionOperations(() -> queryDispatcher.enterQueryMetadataOperation(queryId));
        sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
        QueryStateMachine existing = queries.putIfAbsent(queryId, sm);
        if (existing != null) {
            throw new QueryRejectedException(
                    "Query already registered: queryId=" + queryId
                    + " state=" + existing.getState());
        }
        try {
            queryDispatcher.openQueryIssuance(queryId);
        } catch (RuntimeException | Error failure) {
            queries.remove(queryId, sm);
            throw failure;
        }
        activeQueryCount.incrementAndGet();
        return sm;
    }

    private void registerJournalListener(String queryId, QueryStateMachine sm) {
        journalLifecycle.registerListener(queryId, sm);
    }

    private void applyDispatchResult(String queryId, QueryStateMachine sm, DispatchResult result) {
        if (result != null) {
            sm.setResultEndpoint(result.resultEndpoint());
        }
        if (sm.getState() == QueryStateMachine.State.FINISHED) {
            completeFinishedQuery(queryId, sm);
        }
    }

    private CoordinatorSessionContext enrichWithTenant(CoordinatorSessionContext session) {
        String tenantId = tenantGuard != null
                ? tenantGuard.checkAndResolve(session.userIdentity())
                : (session.tenantId() != null && !session.tenantId().isBlank()
                        ? session.tenantId() : "default");
        return new CoordinatorSessionContext(
                session.queryId(), session.sessionProperties(),
                session.userIdentity(), tenantId);
    }

    public void setClusterAdmissionController(
            org.pangrova.spi.resource.admission.ClusterAdmissionController controller) {
        admission.setClusterController(controller);
    }

    public boolean cancelQuery(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        boolean cancelled = sm != null && sm.tryTransition(QueryStateMachine.State.CANCELLED);
        if (cancelled) {
            completeTerminalQuery(queryId, sm, "user_cancelled");
        } else if (sm != null && sm.getState() == QueryStateMachine.State.CANCELLED) {
            completeTerminalQuery(queryId, sm, "user_cancelled");
        }
        return cancelled;
    }

    public Optional<QueryStateMachine.State> getQueryState(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        return sm != null ? Optional.of(sm.getState()) : Optional.empty();
    }

    public Optional<QueryStateMachine.State> getQueryStateWithJournalFallback(String queryId) {
        Optional<QueryStateMachine.State> local = getQueryState(queryId);
        if (local.isPresent()) {
            return local;
        }
        return journalLifecycle.lookup(queryId);
    }

    public void setLocalCoordinatorId(String coordinatorId) {
        journalLifecycle.setOwner(coordinatorId);
    }

    public Optional<ResultEndpoint> getResultEndpoint(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        return sm != null ? sm.getResultEndpoint() : Optional.empty();
    }

    public QueryStateMachine getStateMachine(String queryId) {
        return queries.get(queryId);
    }

    public void registerStateMachine(String queryId, QueryStateMachine sm) {
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            synchronized (sm) {
                sm.setTransitionOperations(() -> queryDispatcher.enterQueryMetadataOperation(queryId));
                sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
                if (queries.putIfAbsent(queryId, sm) != null) return;
                try { queryDispatcher.openQueryIssuance(queryId); }
                catch (RuntimeException | Error failure) { queries.remove(queryId, sm); throw failure; }
                activeQueryCount.incrementAndGet();
            }
        } finally { queryLifecycleLock.readLock().unlock(); }
        var operation = queryDispatcher.enterQueryMetadataOperation(queryId);
        try {
            if (journalLifecycle.journal().latestQueryEvent(queryId).isPresent()) sm.retainJournalOwnership();
            if (sm.isTerminal()) sm.claimTerminalEffects();
        } finally { operation.close(); }
        if (sm.isTerminal()) completeTerminalQuery(queryId, sm, "registered_terminal");
    }

    public void markRecoveredIssuanceUnresolved(String queryId) { queryDispatcher.markRecoveredIssuanceUnresolved(queryId); }

    public QueryStateJournal getStateJournal() {
        return journalLifecycle.journal();
    }

    public int getActiveQueryCount() {
        return activeQueryCount.get();
    }

    public void setAcceptingQueries(boolean accepting) {
        queryLifecycleLock.writeLock().lock();
        try {
            this.draining = !accepting;
        } finally {
            queryLifecycleLock.writeLock().unlock();
        }
    }

    /** Waits for actual execution and dependent cleanup, not merely logical termination. */
    public boolean awaitAllQueries(long timeoutMs) {
        setAcceptingQueries(false);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (getActiveQueryCount() > 0) {
            terminalCleanup.retryPending();
            drainSignalLock.lock();
            try {
                if (getActiveQueryCount() == 0) break;
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                allQueriesDrained.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                drainSignalLock.unlock();
            }
        }
        return true;
    }

    public Set<String> getActiveQueryIds() {
        return queries.entrySet().stream()
                .filter(e -> !e.getValue().physicalCleanupApplied())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public Map<String, QueryStateMachine> getStateMachines() {
        return Map.copyOf(queries);
    }

    public int cleanupTerminalQueries(Duration retention) {
        return cleanupScheduler.cleanupTerminalQueries(retention);
    }

    public void startPeriodicCleanup(ScheduledExecutorService executor,
                                     Duration retention, Duration interval) {
        cleanupScheduler.startPeriodicCleanup(executor, Runnable::run, retention, interval);
    }

    public void startPeriodicCleanup(ScheduledExecutorService timer,
                                     Executor cleanupExecutor,
                                     Duration retention, Duration interval) {
        cleanupScheduler.startPeriodicCleanup(timer, cleanupExecutor, retention, interval);
    }

    public void stopPeriodicCleanup() {
        cleanupScheduler.stopPeriodicCleanup();
    }

    public void onAllTasksFinished(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return;
        }
        if (sm.getState() == QueryStateMachine.State.FINISHED) {
            completeFinishedQuery(queryId, sm);
            return;
        }
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        if (!sm.tryTransition(QueryStateMachine.State.FINISHED)) {
            return;
        }
        completeFinishedQuery(queryId, sm);
    }

    private void completeFinishedQuery(String queryId, QueryStateMachine sm) {
        completeTerminalQuery(queryId, sm, "all_tasks_finished");
    }

    private void completeTerminalQuery(String queryId, QueryStateMachine sm, String reason) {
        terminalCleanup.complete(queryId, sm, reason);
    }

    private void appendTerminalJournal(QueryTerminalCleanup.Context context) {
        journalLifecycle.terminal(context);
    }

    private void emitTerminalEvents(QueryTerminalCleanup.Context context) { terminalEvents.emit(context); }

    private void releasePhysicalQuery(QueryTerminalCleanup.Context context) {
        journalLifecycle.purge(context.queryId(), context.state());
        context.state().releaseAdmission();
        admission.releaseThrottle(context.queryId());
        queryDispatcher.retireQueryIssuance(context.queryId());
        if (context.state().claimPhysicalCleanup()) {
            activeQueryCount.decrementAndGet();
            if (context.state().discardUnadmittedQuery()) queries.remove(context.queryId(), context.state());
            signalDrainProgress();
        }
    }

    void retryPendingTerminalCompletions() { terminalCleanup.retryPending(); }

    public void onTaskFailed(String queryId, ErrorDetail error) {
        failQuery(queryId, error, "task_failed");
    }

    public void onQueryTimeout(String queryId, ErrorDetail error) {
        failQuery(queryId, error, "query_timeout");
    }

    private void failQuery(String queryId, ErrorDetail error, String reason) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return;
        }
        if (!sm.isTerminal()) {
            if (!sm.tryTransitionToFailed(error)
                    && sm.getState() != QueryStateMachine.State.FAILED) {
                return;
            }
        } else if (sm.getState() != QueryStateMachine.State.FAILED) {
            return;
        }
        completeTerminalQuery(queryId, sm, reason);
    }

    public Optional<ErrorDetail> getQueryError(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return Optional.empty();
        }
        return sm.getError();
    }

    public void emitTableAccessedAudit(String queryId, String catalog, String schema,
                                       String table, CoordinatorSessionContext session) {
        auditBridge.emitTableAccessedAudit(queryId, catalog, schema, table, session);
    }

    private static SubmitQueryResponse buildResponse(
            String queryId, QueryStateMachine sm, DispatchResult result, List<String> columnNames) {
        var endpoint = (result != null)
                ? result.resultEndpoint()
                : sm.getResultEndpoint().orElse(null);
        return new SubmitQueryResponse(
                queryId, sm.getState().name(), endpoint, columnNames);
    }

    private void signalDrainProgress() {
        if (!draining) {
            return;
        }
        drainSignalLock.lock();
        try {
            allQueriesDrained.signalAll();
        } finally {
            drainSignalLock.unlock();
        }
    }
}
