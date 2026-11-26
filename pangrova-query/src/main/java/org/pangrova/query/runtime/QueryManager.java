package org.pangrova.query.runtime;

import org.pangrova.query.retention.QueryRetentionScheduler;

import org.pangrova.query.lifecycle.QueryExecutionPort;
import org.pangrova.query.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.tenant.ResolvedTenantContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.pangrova.query.lifecycle.QueryRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.query.api.lifecycle.QueryOperation;
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
public final class QueryManager implements org.pangrova.query.api.lifecycle.QueryTimeoutHandler,
        org.pangrova.query.recovery.QueryRecoveryTarget {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    private static final String DRAIN_REJECTION_MESSAGE =
            "Coordinator is draining, not accepting new queries";

    private final QueryRegistry queries = new QueryRegistry();
    private final Map<String, QueryStateMachine> pendingCandidateCleanup = new ConcurrentHashMap<>();
    private final Set<QueryStateMachine> candidateCleanupClaims = ConcurrentHashMap.newKeySet();
    private final Set<String> pendingCancellations = ConcurrentHashMap.newKeySet();
    private final QueryTerminalCleanup terminalCleanup;
    private final org.pangrova.query.lifecycle.QueryAdmissionPort admission;
    private final ReentrantReadWriteLock queryLifecycleLock = new ReentrantReadWriteLock();
    private final QueryExecutionPort queryDispatcher;
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
    private final ScheduledExecutorService submitDeadlines;
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
        this.queryDispatcher.addQueryDrainProgressListener(terminalCleanup::onDrainProgress);
        this.auditBridge = java.util.Objects.requireNonNull(deps.audit(), "audit");
        this.admission = java.util.Objects.requireNonNull(deps.admission(), "admission");
        this.journalLifecycle = deps.journal();
        this.cleanupScheduler = new QueryRetentionScheduler(queries,
                terminalCleanup::isPending, this::retryPendingTerminalCompletions,
                journalLifecycle::purgeExpiredAcceptances);
        this.asyncSubmitConfig = deps.asyncSubmitConfig();
        this.asyncExecutor = deps.asyncExecutor();
        this.submitDeadlines = deps.submitDeadlines();
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

    /** Synchronous caller of the same submission receipt used by HTTP. */
    public QuerySubmissionResult submitQuery(String queryId, String sql, QuerySession session) {
        return submitQuery(queryId, sql, session, null);
    }

    /** Synchronously returns the durable acceptance receipt when idempotency material is supplied. */
    public QuerySubmissionResult submitQuery(String queryId, String sql, QuerySession session,
            QueryAcceptanceRequest acceptance) {
        var pending = startSubmission(queryId, sql, session, acceptance);
        try {
            return pending.receipt().get();
        } catch (InterruptedException interrupted) {
            try {
                pending.interrupt().run();
            } finally {
                Thread.currentThread().interrupt();
            }
            throw new QueryRejectedException("Query submission was interrupted");
        } catch (ExecutionException failed) {
            var cause = failed.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new CompletionException(cause);
        }
    }

    /** Admits the query and returns its receipt once the execution owner publishes readiness. */
    public CompletionStage<QuerySubmissionResult> submitQueryAsync(String queryId, String sql, QuerySession session) {
        return submitQueryAsync(queryId, sql, session, null);
    }

    /** Returns the durable acceptance receipt while execution continues under the same owner. */
    public CompletionStage<QuerySubmissionResult> submitQueryAsync(String queryId, String sql, QuerySession session,
            QueryAcceptanceRequest acceptance) {
        return startSubmission(queryId, sql, session, acceptance).receipt().minimalCompletionStage();
    }

    private QueryAsyncSubmission.Pending startSubmission(String queryId, String sql, QuerySession session,
            QueryAcceptanceRequest acceptance) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(sql, "sql must not be null");
        checkNotNull(session, "session must not be null");
        if (acceptance != null && (session.userIdentity() == null
                || !acceptance.principal().equals(session.userIdentity().principal()))) {
            throw new IllegalArgumentException("Acceptance principal does not match the query session");
        }
        QuerySession submittedSession;
        QueryStateMachine stateMachine;
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) {
                throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            }
            submittedSession = session.withQueryId(queryId);
            if (queries.get(queryId) != null) {
                throw new QueryRejectedException("Query already registered: queryId=" + queryId);
            }
            stateMachine = newCandidateStateMachine(queryId, submittedSession, sql);
            if (!queries.reserveSubmission(queryId, stateMachine)) {
                throw new QueryRejectedException("Query submission already pending: queryId=" + queryId);
            }
        } finally {
            queryLifecycleLock.readLock().unlock();
        }
        Span span = null;
        StartedSubmission started;
        try (Scope parent = submittedSession.traceContext().makeCurrent()) {
            span = tracingProvider.startSpan("query.submit", SpanKind.SERVER);
            span.setAttribute("query.id", queryId);
            Map<String, String> previousLogContext = LogContext.snapshot();
            try (Scope current = span.makeCurrent()) {
                submittedSession = submittedSession.withTraceContext(traceContexts.capture());
                LogContext.set(submittedSession.traceContext().traceId(), submittedSession.traceContext().spanId());
                LogContext.setQueryId(queryId);
                auditBridge.prepareSubmittedLineage(queryId, stateMachine.getSql(), submittedSession, stateMachine);
                started = executeQuery(queryId, sql, submittedSession, stateMachine, acceptance);
            } finally {
                LogContext.restore(previousLogContext);
            }
        } catch (RuntimeException | Error failure) {
            recordSubmissionFailure(queryId, stateMachine, failure);
            pendingCancellations.remove(queryId);
            boolean ownsRegistration = queries.get(queryId) == stateMachine;
            if (ownsRegistration) {
                queries.releaseSubmission(queryId, stateMachine);
            } else {
                Throwable cleanupFailure = cleanupRejectedCandidate(queryId, stateMachine, failure);
                if (cleanupFailure != null) {
                    LOG.error("event=query_candidate_cleanup_pending queryId={}", queryId, cleanupFailure);
                }
            }
            boolean accepted = acceptance != null && stateMachine.hasJournalOwnership()
                    && ownsRegistration;
            ResourceCloser.closeAll(failure,
                    () -> {
                        if (ownsRegistration) {
                            finishSubmission(null, queryId, stateMachine, failure);
                        }
                    },
                    ResourceCloser.present(span, Span::end));
            if (accepted) {
                return QueryAsyncSubmission.Pending.completed(
                        QueryAcceptance.pending(acceptance, queryId,
                                QueryAcceptance.DEFAULT_RETENTION_MS).receipt());
            }
            throw failure;
        }

        if (started.pending() == null) {
            span.end();
            return QueryAsyncSubmission.Pending.completed(started.acceptanceReceipt());
        }
        var pending = started.pending();
        var receiptSession = submittedSession;
        var receiptSpan = span;
        var receipt = pending.receipt().whenComplete((result, failure) -> {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            var previousLogContext = LogContext.snapshot();
            try (Scope scope = receiptSession.traceContext().makeCurrent()) {
                LogContext.set(receiptSession.traceContext().traceId(), receiptSession.traceContext().spanId());
                LogContext.setQueryId(queryId);
                if (cause != null) recordSubmissionFailure(queryId, stateMachine, cause);
            } finally {
                ResourceCloser.closeAll(cause,
                        () -> finishSubmission(started.operation(), queryId, stateMachine, cause),
                        receiptSpan::end,
                        () -> LogContext.restore(previousLogContext));
            }
        });
        if (started.acceptanceReceipt() != null) {
            return new QueryAsyncSubmission.Pending(
                    CompletableFuture.completedFuture(started.acceptanceReceipt()), pending.interrupt());
        }
        return new QueryAsyncSubmission.Pending(receipt, pending.interrupt());
    }

    private record StartedSubmission(QueryAsyncSubmission.Pending pending, QueryOperation operation,
            QuerySubmissionResult acceptanceReceipt) { }

    private void recordSubmissionFailure(String queryId, QueryStateMachine state, Throwable failure) {
        state.resolveOwnership(false);
        if (queries.get(queryId) != state) return;
        ResourceCloser.closeAll(failure,
                () -> failQuery(queryId, submissionError("QUERY_SUBMISSION_FAILED", failure),
                        "submission_failed"));
    }

    private static QueryFailure submissionError(String defaultCode, Throwable failure) {
        String code = failure instanceof PangrovaException domain ? domain.getErrorCode().getCode() : defaultCode;
        return new QueryFailure(code, failure.getClass().getSimpleName(), String.valueOf(failure.getMessage()));
    }

    private void finishSubmission(QueryOperation submission,
            String queryId, QueryStateMachine state, Throwable original) {
        try {
            if (submission != null) submission.close();
            if (state.isTerminal()) completeTerminalQuery(queryId, state, "submission_exited");
        } catch (RuntimeException | Error cleanup) {
            if (original == null) throw cleanup;
            if (original != cleanup) original.addSuppressed(cleanup);
        }
    }

    private StartedSubmission executeQuery(String queryId, String sql, QuerySession session,
            QueryStateMachine sm, QueryAcceptanceRequest acceptanceRequest) {
        QuerySession enriched;
        boolean registered = false;
        QueryAcceptanceAdmission acceptanceAdmission = null;
        try {
            ResolvedTenantContext tenant = tenantAdmission == null ? null
                    : tenantAdmission.resolveTenant(session.userIdentity(), queries.states());
            String tenantId = tenant == null ? unscopedTenantId(session) : tenant.tenantId();
            sm.setTenantId(tenantId);
            enriched = session.withTenantId(tenantId);
            admission.checkThrottle(queryId, enriched);
            admission.checkCluster(queryId, tenant, sm);
            var ownership = acquireOwnershipOrReject(queryId, tenantId);
            queryDispatcher.bindConnectorAuthority(sm, queryId);
            sm.resolveOwnership(true);
            applyPendingCancellation(queryId, sm);

            if (acceptanceRequest == null) {
                journalLifecycle.queued(queryId, sm.getSql(), enriched, sm, ownership.anchor());
            } else {
                acceptanceAdmission = journalLifecycle.accept(queryId, sm.getSql(), enriched, sm,
                        acceptanceRequest, QueryAcceptance.DEFAULT_RETENTION_MS, ownership.anchor());
                if (!acceptanceAdmission.newExecution()) {
                    pendingCancellations.remove(queryId);
                    sm.resolveOwnership(false);
                    sm.releaseAdmission();
                    admission.releaseThrottle(queryId);
                    cleanupCandidate(queryId, sm);
                    queries.releaseSubmission(queryId, sm);
                    return new StartedSubmission(null, null, acceptanceAdmission.acceptance().receipt());
                }
            }

            registerCandidateStateMachine(queryId, sm);
            registered = true;
            if (!sm.isTerminal()) {
                enriched = enriched.withQueryAuthority(sm.connectorAuthority())
                        .withPublicationRevision(ownership.publicationRevision());
            }
            registerJournalListener(queryId, sm);
            applyPendingCancellation(queryId, sm);
            if (!sm.isTerminal()) {
                sm.acquireAdmissionHealthProbe(admission::acquireHealthProbe);
            }
            incrementCounter(querySubmitCounter);
            LOG.info("event=query_submitted queryId={}", queryId);
            auditBridge.emitAuditSubmitted(queryId, sm.getSql(), enriched, sm);
        } catch (RuntimeException | Error failure) {
            sm.resolveOwnership(false);
            if (!registered && sm.hasJournalOwnership()) {
                registerCandidateStateMachine(queryId, sm);
                registerJournalListener(queryId, sm);
                registered = true;
            }
            if (!registered) {
                throw failure;
            }
            ResourceCloser.closeAll(failure,
                    () -> failQuery(queryId, submissionError("QUERY_ADMISSION_FAILED", failure),
                            "admission_failed"));
            throw failure;
        }

        QueryOperation operation;
        try {
            applyPendingCancellation(queryId, sm);
            operation = queryDispatcher.enterQueryOperation(queryId);
            queries.releaseSubmission(queryId, sm);
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure,
                    () -> failQuery(queryId, submissionError("QUERY_ADMISSION_FAILED", failure),
                            "admission_failed"));
            throw failure;
        }
        try {
            if (sm.isTerminal()) {
                return new StartedSubmission(QueryAsyncSubmission.Pending.completed(
                        buildResponse(queryId, sm, queryDispatcher.removeColumnNames(queryId))), operation,
                        acceptanceAdmission == null ? null : acceptanceAdmission.acceptance().receipt());
            }
            QueryAsyncSubmission.Pending pending = asyncSubmitConfig != null
                    && asyncSubmitConfig.enabled() && asyncExecutor != null
                    ? executeQueryAsync(queryId, sql, enriched, sm)
                    : executeQuerySync(queryId, sql, enriched, sm);
            return new StartedSubmission(pending, operation,
                    acceptanceAdmission == null ? null : acceptanceAdmission.acceptance().receipt());
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, operation::close,
                    () -> failQuery(queryId, submissionError("INTERNAL_ERROR", failure), "dispatch_failed"));
            throw failure;
        }
    }

    private void applyPendingCancellation(String queryId, QueryStateMachine state) {
        if (pendingCancellations.remove(queryId) && !state.isTerminal()) {
            state.tryTransition(QueryState.CANCELLED);
        }
    }

    private QueryAsyncSubmission.Pending executeQuerySync(String queryId, String sql,
            QuerySession session, QueryStateMachine state) {
        QueryDispatchOutcome result = queryDispatcher.dispatch(state, sql, session);
        applyDispatchResult(queryId, state, result, false);
        return QueryAsyncSubmission.Pending.completed(
                buildResponse(queryId, state, queryDispatcher.removeColumnNames(queryId)));
    }

    private QueryAsyncSubmission.Pending executeQueryAsync(String queryId, String sql,
            QuerySession session, QueryStateMachine state) {
        return new QueryAsyncSubmission(queryDispatcher, asyncSubmitConfig, asyncExecutor, submitDeadlines,
                this::failQuery, this::applyDispatchResult).start(queryId, sql, session, state);
    }

    private QueryExecutionPort.Ownership acquireOwnershipOrReject(String queryId, String tenantId) {
        var ownership = queryDispatcher.acquireQueryOwnership(queryId, tenantId);
        if (!ownership.acquired()) {
            throw new QueryRejectedException(
                    "Failed to acquire query ownership for queryId=" + queryId);
        }
        return ownership;
    }

    private QueryStateMachine newCandidateStateMachine(String queryId, QuerySession session, String sql) {
        QueryStateMachine sm = new QueryStateMachine(true, session != null && session.userIdentity() != null
                ? session.userIdentity().principal() : "anonymous");
        sm.bindSql(org.pangrova.parser.parse.SqlDiagnostics.forReporting(sql));
        sm.setTransitionOperations(() -> () -> { });
        sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
        return sm;
    }

    private void registerCandidateStateMachine(String queryId, QueryStateMachine sm) {
        QueryStateMachine existing = queries.register(queryId, sm, () -> { });
        if (existing != null) {
            throw new QueryRejectedException(
                    "Query already registered: queryId=" + queryId
                    + " state=" + existing.getState());
        }
        sm.setTransitionOperations(() -> queryDispatcher.enterQueryMetadataOperation(queryId));
    }

    private Throwable cleanupRejectedCandidate(String queryId, QueryStateMachine state, Throwable original) {
        // Drain and periodic retry must not perform query-ID scoped releases for
        // the same candidate concurrently, or for a candidate already retired.
        if (!candidateCleanupClaims.add(state)) return null;
        try {
            if (queries.pendingSubmission(queryId) != state) return null;
            return cleanupClaimedCandidate(queryId, state, original);
        } finally {
            candidateCleanupClaims.remove(state);
        }
    }

    private Throwable cleanupClaimedCandidate(String queryId, QueryStateMachine state, Throwable original) {
        Throwable cleanupFailure = null;
        try {
            state.releaseAdmission();
        } catch (RuntimeException | Error rejected) {
            cleanupFailure = rejected;
        }
        try {
            admission.releaseThrottle(queryId);
        } catch (RuntimeException | Error rejected) {
            if (cleanupFailure == null) cleanupFailure = rejected;
            else cleanupFailure.addSuppressed(rejected);
        }
        if (queries.get(queryId) == null) {
            try {
                cleanupCandidate(queryId, state);
            } catch (RuntimeException | Error rejected) {
                if (cleanupFailure == null) cleanupFailure = rejected;
                else cleanupFailure.addSuppressed(rejected);
            }
        }
        if (cleanupFailure == null) {
            pendingCandidateCleanup.remove(queryId, state);
            queries.releaseSubmission(queryId, state);
            return null;
        }
        pendingCandidateCleanup.put(queryId, state);
        if (original != null) original.addSuppressed(cleanupFailure);
        if (cleanupFailure instanceof Error error) throw error;
        return cleanupFailure;
    }

    private void retryPendingCandidateCleanups() {
        for (var entry : pendingCandidateCleanup.entrySet()) {
            try {
                cleanupRejectedCandidate(entry.getKey(), entry.getValue(), null);
            } catch (RuntimeException failure) {
                LOG.warn("event=query_candidate_cleanup_retry_failed queryId={}", entry.getKey(), failure);
            }
        }
    }

    private void cleanupCandidate(String queryId, QueryStateMachine state) {
        try {
            if (state.hasConnectorAuthority()) {
                if (!queryDispatcher.tryCleanupUnadmittedQuery(queryId)) {
                    throw new IllegalStateException("Candidate issuance cleanup is not complete: " + queryId);
                }
                queryDispatcher.retireQueryIssuance(queryId);
            }
        } finally {
            queryDispatcher.releaseQueryOwnership(queryId);
        }
    }

    private void registerJournalListener(String queryId, QueryStateMachine sm) {
        journalLifecycle.registerListener(queryId, sm);
    }

    private void applyDispatchResult(String queryId, QueryStateMachine sm, QueryDispatchOutcome result) {
        applyDispatchResult(queryId, sm, result, true);
    }

    private void applyDispatchResult(String queryId, QueryStateMachine sm, QueryDispatchOutcome result,
            boolean completeFinished) {
        java.util.Objects.requireNonNull(result, "dispatch outcome");
        switch (result.resultDisposition()) {
            case STREAM -> sm.setResultEndpoint(result.resultEndpoint());
            case NONE -> sm.markNoResult();
            case PENDING -> { }
            default -> throw new IllegalArgumentException(
                    "Unknown result disposition: " + result.resultDisposition());
        }
        if (completeFinished && sm.getState() == QueryState.FINISHED) {
            completeFinishedQuery(queryId, sm);
        }
    }

    private static String unscopedTenantId(QuerySession session) {
        return session.tenantId() == null || session.tenantId().isBlank() ? "default" : session.tenantId();
    }

    public void setClusterAdmissionController(
            org.pangrova.scheduler.api.admission.ClusterAdmissionController controller) {
        admission.setClusterController(controller);
    }

    /** Requests cancellation and completes terminal effects for the query. */
    public boolean cancelQuery(String queryId) {
        QueryStateMachine pending = queries.pendingSubmission(queryId);
        if (pending != null) {
            pendingCancellations.add(queryId);
            boolean cancelled = pending.tryTransition(QueryState.CANCELLED);
            pendingCancellations.remove(queryId);
            return cancelled || pending.getState() == QueryState.CANCELLED;
        }
        QueryStateMachine registered = queries.get(queryId);
        boolean cancelled = registered != null && registered.tryTransition(QueryState.CANCELLED);
        if (registered != null) {
            if (cancelled) {
                completeTerminalQuery(queryId, registered, "user_cancelled");
            } else if (registered.getState() == QueryState.CANCELLED) {
                completeTerminalQuery(queryId, registered, "user_cancelled");
            }
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

    /** One scoped history lookup for the explicit journal diagnostic path. */
    public Optional<QueryState> getHistoricalQueryState(String queryId, String principal) {
        if (principal == null || principal.isBlank()) return Optional.empty();
        return journalLifecycle.lookupForPrincipal(queryId, principal);
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

    @Override
    public void registerRecoveredStateMachine(String queryId, QueryStateMachine sm) {
        queryDispatcher.markRecoveredIssuanceUnresolved(queryId);
        sm.retainJournalOwnership();
        registerStateMachine(queryId, sm);
    }

    /** Registers a query state under the current lifecycle gate. */
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
            if (!sm.hasJournalOwnership() && journalLifecycle.hasLatestEvent(queryId)) {
                sm.retainJournalOwnership();
            }
            if (sm.isTerminal()) sm.claimTerminalEffects();
        } finally {
            operation.close();
        }
        if (sm.isTerminal()) completeTerminalQuery(queryId, sm, "registered_terminal");
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
        return queries.awaitDrain(timeoutMs, this::retryPendingCleanup);
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
        retryPendingCleanup();
    }

    private void retryPendingCleanup() {
        retryPendingCandidateCleanups();
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
            String queryId, QueryStateMachine sm, List<String> columnNames) {
        var status = sm.status();
        return new QuerySubmissionResult(queryId, status.state(), status.disposition(),
                status.endpoint(), columnNames, sm.branchSelection(), null);
    }
}
