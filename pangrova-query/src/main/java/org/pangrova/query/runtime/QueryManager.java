package org.pangrova.query.runtime;

import org.pangrova.query.retention.QueryRetentionScheduler;

import org.pangrova.query.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.session.QuerySession;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.pangrova.query.lifecycle.QueryRegistry;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.query.api.retention.ReaderPinLifecycle;
import org.pangrova.query.retention.ReaderPinRenewal;
import org.pangrova.query.lifecycle.QueryDispatchOutcome;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.enums.SpanKind;
import org.pangrova.observability.api.tracing.Scope;
import org.pangrova.observability.api.tracing.TracingProvider;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.observability.logging.LogContext;

@ThreadSafe
public class QueryManager implements org.pangrova.query.api.lifecycle.QueryTimeoutHandler,
        org.pangrova.query.recovery.QueryRecoveryTarget {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    private static final String DRAIN_REJECTION_MESSAGE =
            "Coordinator is draining, not accepting new queries";

    private final QueryRegistry queries = new QueryRegistry();
    private final QueryTerminalCleanup terminalCleanup;
    private final org.pangrova.query.lifecycle.QueryAdmissionPort admission;
    private final ReentrantReadWriteLock queryLifecycleLock = new ReentrantReadWriteLock();
    private final org.pangrova.query.lifecycle.QueryExecutionPort queryDispatcher;
    private final TracingProvider tracingProvider;
    private final ExecutionTraceContextFactory traceContexts;
    private final org.pangrova.query.lifecycle.TenantAdmissionPort tenantAdmission;
    private final QueryRetentionScheduler cleanupScheduler;
    private final org.pangrova.query.lifecycle.QueryAuditPort auditBridge;
    private final LongCounter querySubmitCounter;
    private final QueryTerminalEvents terminalEvents;
    private volatile boolean draining;
    private final org.pangrova.query.lifecycle.QueryJournalLifecyclePort journalLifecycle;
    private final AsyncSubmitConfig asyncSubmitConfig;
    private final ExecutorService asyncExecutor;
    private volatile java.util.function.Consumer<String> shuffleCleanupPending = ignored -> { };
    private final Optional<ReaderPinLifecycle> readerPinLifecycle;
    private final Optional<ReaderPinRenewal> readerPinRenewal;

    public QueryManager(QueryManagerContext deps) {
        this(deps, Optional.empty(), System::nanoTime);
    }

    public QueryManager(QueryManagerContext deps, Optional<ReaderPinLifecycle> readerPins) {
        this(deps, readerPins, System::nanoTime);
    }

    /** Creates a manager with an injected monotonic clock and no reader-pin owner. */
    public QueryManager(QueryManagerContext deps, LongSupplier nanoClock) {
        this(deps, Optional.empty(), nanoClock);
    }

    /** Creates a manager with the exact reader-pin owner and monotonic clock. */
    public QueryManager(QueryManagerContext deps, Optional<ReaderPinLifecycle> readerPins, LongSupplier nanoClock) {
        java.util.Objects.requireNonNull(nanoClock, "nanoClock");
        this.readerPinLifecycle = java.util.Objects.requireNonNull(readerPins, "readerPins");
        this.readerPinRenewal = readerPins.map(lifecycle ->
                new ReaderPinRenewal(lifecycle, this::getActiveQueryIds, nanoClock));
        this.queryDispatcher = java.util.Objects.requireNonNull(deps.queryDispatcher(), "queryDispatcher");
        this.tracingProvider = deps.tracingProvider();
        this.traceContexts = deps.traceContexts();
        this.tenantAdmission = deps.tenantAdmission();
        this.terminalCleanup = new QueryTerminalCleanup(queryDispatcher, this::appendTerminalJournal,
                this::emitTerminalEvents,
                this::releasePhysicalQuery, nanoClock);
        this.queryDispatcher.addExecutionDrainListener(terminalCleanup::onExecutionDrain);
        this.queryDispatcher.addQueryOperationExitListener(terminalCleanup::onOperationExit);
        this.cleanupScheduler = new QueryRetentionScheduler(queries,
                terminalCleanup::isPending, this::retryPendingTerminalCompletions);
        this.auditBridge = java.util.Objects.requireNonNull(deps.audit(), "audit");
        this.admission = java.util.Objects.requireNonNull(deps.admission(), "admission");
        this.journalLifecycle = deps.journal();
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

    /** Admits and submits a query under its execution authority. */
    public QuerySubmissionResult submitQuery(String queryId, String sql, QuerySession session) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(sql, "sql must not be null");
        checkNotNull(session, "session must not be null");
        QuerySession submittedSession;
        QueryStateMachine stateMachine;
        org.pangrova.query.api.lifecycle.QueryOperation submission;
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) {
                throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            }
            submittedSession = session.withQueryId(queryId);
            stateMachine = resolveOrCreateStateMachine(queryId, submittedSession, sql);
            submission = queryDispatcher.enterQueryOperation(queryId);
        } finally {
            queryLifecycleLock.readLock().unlock();
        }
        Throwable submissionFailure = null;
        try {
            try (Scope parent = submittedSession.traceContext().makeCurrent()) {
                Span span = tracingProvider.startSpan("query.submit", SpanKind.SERVER);
                span.setAttribute("query.id", queryId);
                Map<String, String> previousLogContext = LogContext.snapshot();
                try (Scope current = span.makeCurrent()) {
                    submittedSession = submittedSession.withTraceContext(
                            traceContexts.capture());
                    LogContext.set(submittedSession.traceContext().traceId(), submittedSession.traceContext().spanId());
                    LogContext.setQueryId(queryId);
                    incrementCounter(querySubmitCounter);
                    LOG.info("event=query_submitted queryId={}", queryId);
                    auditBridge.emitAuditSubmitted(queryId, stateMachine.getSql(), submittedSession, stateMachine);
                    admission.checkDegradation();
                    return executeQuery(queryId, sql, submittedSession, stateMachine);
                } finally {
                    LogContext.restore(previousLogContext);
                    span.end();
                }
            }
        } catch (RuntimeException | Error failure) {
            submissionFailure = failure;
            stateMachine.resolveOwnership(false);
            try {
                failQuery(queryId, new QueryFailure("QUERY_SUBMISSION_FAILED", failure.getClass().getSimpleName(),
                        String.valueOf(failure.getMessage())), "submission_failed");
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        } finally {
            finishSubmission(submission, queryId, stateMachine, submissionFailure);
        }
    }

    private void finishSubmission(org.pangrova.query.api.lifecycle.QueryOperation submission,
            String queryId, QueryStateMachine state, Throwable original) {
        try {
            submission.close();
            if (state.isTerminal()) completeTerminalQuery(queryId, state, "submission_exited");
        } catch (RuntimeException | Error cleanup) {
            if (original == null) throw cleanup;
            if (original != cleanup) original.addSuppressed(cleanup);
        }
    }

    private QuerySubmissionResult executeQuery(String queryId, String sql, QuerySession session,
                                             QueryStateMachine sm) {
        QuerySession enriched;
        try {
            registerJournalListener(queryId, sm);

            enriched = enrichTenant(session.withQueryAuthority(sm.connectorAuthority()), sm);
            admission.checkThrottle(queryId, enriched);

            admission.checkCluster(queryId, enriched.tenantId(), sm);

            acquireOwnershipOrReject(queryId);

            journalLifecycle.queued(queryId, sm.getSql(), enriched, sm);

            sm.resolveOwnership(true);
        } catch (RuntimeException | Error failure) {
            sm.resolveOwnership(false);
            try {
                failQuery(queryId, new QueryFailure("QUERY_ADMISSION_FAILED", failure.getClass().getSimpleName(),
                        String.valueOf(failure.getMessage())), "admission_failed");
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }

        if (asyncSubmitConfig != null && asyncSubmitConfig.enabled() && asyncExecutor != null) {
            return executeQueryAsync(queryId, sql, enriched, sm);
        }

        QueryDispatchOutcome result;
        try {
            result = queryDispatcher.dispatch(sm, sql, enriched);
        } catch (RuntimeException failure) {
            failQuery(queryId, new QueryFailure(
                    "INTERNAL_ERROR", failure.getClass().getSimpleName(),
                    failure.getMessage()), "dispatch_failed");
            throw failure;
        }
        applyDispatchResult(queryId, sm, result);
        return buildResponse(queryId, sm, result, queryDispatcher.removeColumnNames(queryId));
    }

    private QuerySubmissionResult executeQueryAsync(String queryId, String sql,
                                                 QuerySession session, QueryStateMachine state) {
        return new QueryAsyncSubmission(queryDispatcher, asyncSubmitConfig, asyncExecutor,
                this::failQuery, this::applyDispatchResult).execute(queryId, sql, session, state);
    }

    private QuerySession enrichTenant(
            QuerySession session, QueryStateMachine sm) {
        QuerySession enriched = enrichWithTenant(session);
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

    private QueryStateMachine resolveOrCreateStateMachine(String queryId, QuerySession session, String sql) {
        QueryStateMachine sm = new QueryStateMachine(true, session != null && session.userIdentity() != null
                ? session.userIdentity().principal() : "anonymous");
        sm.bindSql(org.pangrova.parser.parse.SqlDiagnostics.forReporting(sql));
        sm.setTransitionOperations(() -> queryDispatcher.enterQueryMetadataOperation(queryId));
        sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
        QueryStateMachine existing = queries.register(queryId, sm,
                () -> queryDispatcher.bindConnectorAuthority(sm, queryId));
        if (existing != null) {
            throw new QueryRejectedException(
                    "Query already registered: queryId=" + queryId
                    + " state=" + existing.getState());
        }
        return sm;
    }

    private void registerJournalListener(String queryId, QueryStateMachine sm) {
        journalLifecycle.registerListener(queryId, sm);
    }

    private void applyDispatchResult(String queryId, QueryStateMachine sm, QueryDispatchOutcome result) {
        if (result != null) {
            sm.setResultEndpoint(result.resultEndpoint());
        }
        if (sm.getState() == QueryState.FINISHED) {
            completeFinishedQuery(queryId, sm);
        }
    }

    private QuerySession enrichWithTenant(QuerySession session) {
        String tenantId = tenantAdmission != null
                ? tenantAdmission.resolveTenant(session.userIdentity(), queries.states())
                : (session.tenantId() != null && !session.tenantId().isBlank()
                        ? session.tenantId() : "default");
        return session.withTenantId(tenantId);
    }

    public void setClusterAdmissionController(
            org.pangrova.scheduler.api.admission.ClusterAdmissionController controller) {
        admission.setClusterController(controller);
    }

    /** Requests cancellation and completes terminal effects for the query. */
    public boolean cancelQuery(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        boolean cancelled = sm != null && sm.tryTransition(QueryState.CANCELLED);
        if (cancelled) {
            completeTerminalQuery(queryId, sm, "user_cancelled");
        } else if (sm != null && sm.getState() == QueryState.CANCELLED) {
            completeTerminalQuery(queryId, sm, "user_cancelled");
        }
        return cancelled;
    }

    public Optional<QueryState> getQueryState(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        return sm != null ? Optional.of(sm.getState()) : Optional.empty();
    }

    /** Looks up local state, then the durable journal when no local owner exists. */
    public Optional<QueryState> getQueryStateWithJournalFallback(String queryId) {
        Optional<QueryState> local = getQueryState(queryId);
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

    /** Registers a recovered query state under the current lifecycle gate. */
    public void registerStateMachine(String queryId, QueryStateMachine sm) {
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            synchronized (sm) {
                sm.setTransitionOperations(() -> queryDispatcher.enterQueryMetadataOperation(queryId));
                sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
                if (queries.register(queryId, sm, () -> {
                    if (sm.isTerminal()) queryDispatcher.openQueryIssuance(queryId);
                    else queryDispatcher.bindConnectorAuthority(sm, queryId);
                }) != null) return;
            }
        } finally {
            queryLifecycleLock.readLock().unlock();
        }
        var operation = queryDispatcher.enterQueryMetadataOperation(queryId);
        try {
            if (journalLifecycle.hasLatestEvent(queryId)) sm.retainJournalOwnership();
            if (sm.isTerminal()) sm.claimTerminalEffects();
        } finally {
            operation.close();
        }
        if (sm.isTerminal()) completeTerminalQuery(queryId, sm, "registered_terminal");
    }

    /** Marks recovered issuance unresolved until its original operations are reconciled. */
    public void markRecoveredIssuanceUnresolved(String queryId) {
        queryDispatcher.markRecoveredIssuanceUnresolved(queryId);
    }

    public org.pangrova.query.lifecycle.QueryStateJournalPort getStateJournal() {
        return journalLifecycle;
    }

    public int getActiveQueryCount() {
        return queries.outstandingCount();
    }

    /** Opens or seals query admission while holding the lifecycle write lock. */
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
        return queries.awaitDrain(timeoutMs, terminalCleanup::retryPending);
    }

    /** Returns query IDs whose physical cleanup remains outstanding. */
    public Set<String> getActiveQueryIds() {
        return queries.outstandingIds();
    }

    public Map<String, QueryStateMachine> getStateMachines() {
        return queries.snapshot();
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
        terminalCleanup.startRetryDispatch(timer, cleanupExecutor);
    }

    public void startReaderPinRenewal(ScheduledExecutorService timer, Executor renewalExecutor) {
        readerPinRenewal.ifPresent(renewal -> renewal.start(timer, renewalExecutor));
    }

    public void stopPeriodicCleanup() {
        readerPinRenewal.ifPresent(ReaderPinRenewal::close);
        terminalCleanup.stopRetryDispatch();
        cleanupScheduler.stopPeriodicCleanup();
    }

    /** Applies logical completion after all task and write effects are ready. */
    public void onAllTasksFinished(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return;
        }
        if (sm.getState() == QueryState.FINISHED) {
            completeFinishedQuery(queryId, sm);
            return;
        }
        if (sm.signalNativeWriteCompletedForControlPlane()) {
            return;
        }
        sm.tryTransition(QueryState.RUNNING);
        sm.tryTransition(QueryState.FINISHING);
        if (!sm.tryTransition(QueryState.FINISHED)) {
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
        readerPinLifecycle.ifPresent(lifecycle -> lifecycle.release(context.queryId()));
        journalLifecycle.purge(context.queryId(), context.state());
        context.state().releaseAdmission();
        admission.releaseThrottle(context.queryId());
        queryDispatcher.retireQueryIssuance(context.queryId());
        queries.complete(context.queryId(), context.state());
    }

    /** Advances exact-attempt seals even when their query is still running. */
    public void retryPendingTaskCancellations() {
        queryDispatcher.retryPendingCancels();
    }

    public void retryPendingTerminalCompletions() {
        readerPinRenewal.ifPresent(ReaderPinRenewal::renewDue);
        terminalCleanup.retryPending();
    }

    public void onTaskFailed(String queryId, QueryFailure error) {
        failQuery(queryId, error, "task_failed");
    }

    /** Timeout selection and terminal transition share the actual query owner. */
    @Override
    public void timeoutQueriesCreatedBy(java.time.Instant deadline, Duration timeout) {
        java.util.Objects.requireNonNull(deadline, "deadline");
        java.util.Objects.requireNonNull(timeout, "timeout");
        queries.forEach((queryId, state) -> {
            if (state.isTerminal() || state.getCreatedAt().isAfter(deadline)) return;
            QueryFailure error = new QueryFailure("QUERY_TIMEOUT", "TIMEOUT",
                    "Query " + queryId + " exceeded timeout of " + timeout);
            // Retain this observed lifetime. A concurrent winner owns its terminal
            // path; never look up a later registration by query ID to apply timeout.
            if (state.tryTransitionToFailed(error)) {
                completeTerminalQuery(queryId, state, "query_timeout");
                LOG.warn("event=query_timeout queryId={} timeout={} delegated=true", queryId, timeout);
            }
        });
    }

    private void failQuery(String queryId, QueryFailure error, String reason) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return;
        }
        if (!sm.isTerminal()) {
            if (!sm.tryTransitionToFailed(error)
                    && sm.getState() != QueryState.FAILED) {
                return;
            }
        } else if (sm.getState() != QueryState.FAILED) {
            return;
        }
        completeTerminalQuery(queryId, sm, reason);
    }

    /** Returns the query's recorded error when present. */
    public Optional<QueryFailure> getQueryError(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return Optional.empty();
        }
        return sm.getError();
    }

    public void emitTableAccessedAudit(String queryId, String catalog, String schema,
                                       String table, QuerySession session) {
        auditBridge.emitTableAccessedAudit(queryId, catalog, schema, table, session);
    }

    private static QuerySubmissionResult buildResponse(
            String queryId, QueryStateMachine sm, QueryDispatchOutcome result, List<String> columnNames) {
        var endpoint = (result != null)
                ? result.resultEndpoint()
                : sm.getResultEndpoint().orElse(null);
        return new QuerySubmissionResult(
                queryId, sm.getState(), endpoint, columnNames, sm.branchSelection());
    }
}
