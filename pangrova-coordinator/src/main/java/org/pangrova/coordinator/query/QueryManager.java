package org.pangrova.coordinator.query;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.planner.throttle.ThrottlePermit;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.coordination.QueryStateEvent;
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

/**
 * Manages query lifecycle state machines.
 * One QueryStateMachine per active query.
 *
 * <p>Cleanup logic delegated to {@link QueryCleanupScheduler}.
 * Audit/lineage emission delegated to {@link QueryAuditDispatcher}.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for query registry.
 * A {@link ReentrantReadWriteLock} coordinates drain vs normal operations:
 * read lock for query submit/complete/cancel (concurrent), write lock for
 * drain (exclusive). This eliminates the deadlock window that existed with
 * the previous drainLock+CompletableFuture approach.</p>
 */
@ThreadSafe
public class QueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    private static final String DRAIN_REJECTION_MESSAGE =
            "Coordinator is draining, not accepting new queries";

    private final ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
    private static final int MAX_TERMINAL_RETRIES_PER_TICK = 4;
    private static final long INITIAL_RETRY_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(30);

    /** Terminal journal/dispatch cleanup still needing a local retry. */
    private final ConcurrentHashMap<String, TerminalCompletion> pendingTerminalCompletion =
            new ConcurrentHashMap<>();
    private final PriorityQueue<RetryTicket> terminalRetryQueue = new PriorityQueue<>(
            java.util.Comparator.comparingLong(RetryTicket::dueAtNanos));
    private final LongSupplier nanoClock;

    private static final class TerminalCompletion {
        private final QueryStateMachine stateMachine;
        private final String reason;
        private final AtomicBoolean inProgress = new AtomicBoolean();
        private boolean retryQueued;
        private int retryFailures;

        private TerminalCompletion(QueryStateMachine stateMachine, String reason) {
            this.stateMachine = stateMachine;
            this.reason = reason;
        }
    }

    private record RetryTicket(String queryId, TerminalCompletion completion,
                               long dueAtNanos) { }
    // PERF-T17: Maintain O(1) active query count instead of O(N) stream traversal.
    private final AtomicInteger activeQueryCount = new AtomicInteger(0);
    /** Tracks throttle permits by queryId so they can be released on terminal state. */
    private final ConcurrentHashMap<String, ThrottlePermit> throttlePermits = new ConcurrentHashMap<>();

    /**
     * Read lock: held by normal query operations (submit, complete, cancel, fail).
     * Write lock: held exclusively by drain to block new queries.
     */
    @javax.annotation.concurrent.GuardedBy("itself")
    private final ReentrantReadWriteLock queryLifecycleLock = new ReentrantReadWriteLock();
    // Separate lock for drain condition signaling to avoid the lost-wakeup race
    // where tryLock on the write lock fails because a reader holds the read lock
    // (F-COORD-14).
    private final java.util.concurrent.locks.ReentrantLock drainSignalLock =
            new java.util.concurrent.locks.ReentrantLock();
    private final Condition allQueriesDrained = drainSignalLock.newCondition();

    private final QueryDispatcher queryDispatcher;
    private final TracingProvider tracingProvider;
    private final TenantQueryGuard tenantGuard;
    private final QueryCleanupScheduler cleanupScheduler;
    private final QueryAuditDispatcher auditBridge;
    private final LongCounter querySubmitCounter;
    private final LongCounter queryCompleteCounter;
    private final LongCounter queryFailCounter;
    @javax.annotation.concurrent.GuardedBy("queryLifecycleLock")
    private boolean draining;
    private final org.pangrova.resilience.ResilienceRegistry resilienceRegistry;
    private final org.pangrova.planner.throttle.QueryThrottlingService throttlingService;
    private final QueryStateJournal stateJournal;
    private final AsyncSubmitConfig asyncSubmitConfig;
    private final ExecutorService asyncExecutor;
    private volatile org.pangrova.spi.resource.admission.ClusterAdmissionController clusterAdmissionController;
    private volatile java.util.function.Consumer<String> shuffleCleanupPending = ignored -> { };
    /**
     * SI-6 / A-FENCE-01: this coordinator's node id, stamped on every journal event so
     * owner-scoped failover recovery ({@code replayActiveQueriesForOwner}) can filter
     * rows by owning coordinator. Null before HA bootstrap wires it (single-node mode).
     */
    private volatile String localCoordinatorId;

    /** Primary constructor accepting a dependencies parameter object. */
    public QueryManager(QueryManagerContext deps) {
        this(deps, System::nanoTime);
    }

    QueryManager(QueryManagerContext deps, LongSupplier nanoClock) {
        this.nanoClock = java.util.Objects.requireNonNull(nanoClock, "nanoClock");
        this.queryDispatcher = deps.queryDispatcher();
        this.tracingProvider = deps.tracingProvider();
        this.tenantGuard = (deps.tenantResolver() != null && deps.quotaChecker() != null)
                ? new TenantQueryGuard(deps.tenantResolver(), deps.quotaChecker(),
                        tenantId -> {
                            int concurrent = 0;
                            int queued = 0;
                            for (QueryStateMachine sm : queries.values()) {
                                if (sm.isTerminal() || !tenantId.equals(sm.getTenantId())) {
                                    continue;
                                }
                                concurrent++;
                                if (sm.getState() == QueryStateMachine.State.QUEUED) {
                                    queued++;
                                }
                            }
                            return new org.pangrova.security.tenant.quota.TenantUsage(
                                    concurrent, queued, 0L, 0L);
                        })
                : null;
        this.cleanupScheduler = new QueryCleanupScheduler(queries,
                pendingTerminalCompletion::containsKey,
                this::retryPendingTerminalCompletions);
        this.auditBridge = new QueryAuditDispatcher(
                QueryAuditDispatcher.createAuditEmitter(deps.auditEmitter(), deps.queryAuditService()),
                deps.lineageEmitter(),
                deps.queryDispatcher());
        this.resilienceRegistry = deps.resilienceRegistry();
        this.throttlingService = deps.throttlingService();
        this.stateJournal = deps.stateJournal() != null
                ? deps.stateJournal() : InMemoryQueryStateJournal.INSTANCE;
        this.asyncSubmitConfig = deps.asyncSubmitConfig();
        this.asyncExecutor = deps.asyncExecutor();
        this.querySubmitCounter = createCounter(deps.metricsProvider(),
                "pangrova.coordinator.queries.submitted", "Total queries submitted", "queries");
        this.queryCompleteCounter = createCounter(deps.metricsProvider(),
                "pangrova.coordinator.queries.completed", "Total queries completed", "queries");
        this.queryFailCounter = createCounter(deps.metricsProvider(),
                "pangrova.coordinator.queries.failed", "Total queries failed", "queries");
    }

    private static LongCounter createCounter(MetricsProvider provider,
                                              String name, String description, String unit) {
        if (provider == null) {
            return null;
        }
        return provider.createCounter(name, description, unit);
    }

    /** Wires durable Shuffle cleanup intent before terminal journal publication. */
    public void setShuffleCleanupPending(java.util.function.Consumer<String> recorder) {
        shuffleCleanupPending = java.util.Objects.requireNonNull(recorder, "recorder");
        queries.forEach((queryId, sm) -> sm.setBeforeTerminalTransition(
                () -> shuffleCleanupPending.accept(queryId)));
    }

    private void incrementCounter(LongCounter counter) {
        if (counter != null) {
            counter.add(1);
        }
    }

    /**
     * Submits a new query, triggers orchestration (compile -> schedule -> dispatch),
     * and returns a response with queryId, state, and result endpoint.
     *
     * <p>Acquires the read lock to allow concurrent submissions while blocking
     * during drain (write lock held by {@link #awaitAllQueries(long)}).</p>
     */
    public SubmitQueryResponse submitQuery(String queryId, String sql, CoordinatorSessionContext session) {
        queryLifecycleLock.readLock().lock();
        try {
            if (draining) {
                throw new QueryRejectedException(DRAIN_REJECTION_MESSAGE);
            }
            checkDegradation();
            checkNotNull(queryId, "queryId must not be null");
            checkNotNull(sql, "sql must not be null");
            checkNotNull(session, "session must not be null");
            Span span = tracingProvider.startSpan("query.submit", SpanKind.SERVER);
            span.setAttribute("query.id", queryId);
            LogContext.set(NativeTraceExporter.currentTraceId(), NativeTraceExporter.currentSpanId());
            try {
                incrementCounter(querySubmitCounter);
                LOG.info("event=query_submitted queryId={}", queryId);
                auditBridge.emitAuditSubmitted(queryId, sql, session);
                return executeQuery(queryId, sql, session);
            } catch (Exception e) {
                // Release throttle permit on early failure before the query
                // reaches a terminal state handler (which would release it).
                releaseThrottlePermit(queryId);
                throw e;
            } finally {
                LogContext.clear();
                span.end();
            }
        } finally {
            queryLifecycleLock.readLock().unlock();
        }
    }

    private SubmitQueryResponse executeQuery(String queryId, String sql, CoordinatorSessionContext session) {
        QueryStateMachine sm = resolveOrCreateStateMachine(queryId, session);
        CoordinatorSessionContext enriched;
        boolean ownershipAcquired = false;
        Runnable releaseEarlyAdmission = null;
        try {
            registerJournalListener(queryId, sm);

            enriched = enrichTenant(session, sm);
            checkThrottle(queryId, enriched);

            // P1-5b: cluster-wide admission gate — rejects queries when the tenant's
            // global usage (heartbeat-fed) exceeds configured limits, preventing
            // aggregate oversell across coordinators.
            releaseEarlyAdmission = checkClusterAdmission(queryId, enriched.tenantId(), sm);

            acquireOwnershipOrReject(queryId);
            ownershipAcquired = true;

            stateJournal.appendQueryEventSync(new QueryStateEvent(
                    queryId, sql, serializeSession(enriched),
                    QueryStateMachine.State.QUEUED.name(), System.currentTimeMillis(),
                    localCoordinatorId));

            sm.resolveOwnership(true);
        } catch (RuntimeException | Error failure) {
            if (releaseEarlyAdmission != null) {
                try {
                    releaseEarlyAdmission.run();
                } catch (RuntimeException releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            try {
                if (queries.remove(queryId, sm)) {
                    activeQueryCount.decrementAndGet();
                }
                if (ownershipAcquired) {
                    queryDispatcher.cleanupQuery(queryId);
                }
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            } finally {
                sm.resolveOwnership(false);
            }
            throw failure;
        }

        // Task 186: if async submit is enabled, dispatch execution to a
        // background executor and return RUNNING as soon as the OUTPUT
        // stage is dispatched rather than blocking until full DAG completion.
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

    /**
     * Task 186: Async query execution -- submits execution to background
     * executor and returns once an endpoint is published or the query settles.
     */
    private SubmitQueryResponse executeQueryAsync(String queryId, String sql,
                                                   CoordinatorSessionContext enriched,
                                                   QueryStateMachine sm) {
        CountDownLatch readyLatch = new CountDownLatch(1);
        AtomicReference<RuntimeException> dispatchFailure = new AtomicReference<>();
        sm.resultEndpointReady().thenRun(readyLatch::countDown);
        sm.addStateChangeListener((previous, current) -> {
            if ((current == QueryStateMachine.State.FAILED
                    && sm.getError().isPresent())
                    || current == QueryStateMachine.State.CANCELLED) {
                readyLatch.countDown();
            }
        });

        Future<?> dispatchFuture;
        try {
            dispatchFuture = asyncExecutor.submit(() -> {
                try {
                    DispatchResult result = queryDispatcher.execute(sm, sql, enriched);
                    applyDispatchResult(queryId, sm, result);
                } catch (Exception e) {
                    dispatchFailure.set(e instanceof RuntimeException runtime
                            ? runtime : new IllegalStateException(e));
                    LOG.error("event=async_query_execution_failed queryId={} error={}",
                            queryId, e.getMessage(), e);
                    failQuery(queryId, new ErrorDetail(
                            "INTERNAL_ERROR", e.getClass().getSimpleName(), e.getMessage()),
                            "async_dispatch_failed");
                } finally {
                    // A control-plane statement may settle before its DispatchResult
                    // publishes the endpoint. Wake only after that result is applied.
                    readyLatch.countDown();
                }
            });
        } catch (RejectedExecutionException overloaded) {
            failQuery(queryId, new ErrorDetail(
                    "RESOURCE_EXHAUSTED", "ASYNC_SUBMIT_QUEUE_FULL",
                    "Coordinator query dispatch queue is full"), "async_submit_overloaded");
            throw new QueryRejectedException("Coordinator query dispatch queue is full");
        }

        try {
            boolean reached = readyLatch.await(
                    asyncSubmitConfig.submitTimeoutMs(), TimeUnit.MILLISECONDS);
            if (!reached) {
                dispatchFuture.cancel(true);
                failQuery(queryId, new ErrorDetail(
                        "QUERY_TIMEOUT", "ASYNC_SUBMIT_TIMEOUT",
                        "Result endpoint was not ready before submit timeout"),
                        "async_submit_timeout");
                throw new org.pangrova.common.exception.PangrovaException(
                        org.pangrova.common.exception.PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT,
                        "Result endpoint was not ready before submit timeout: " + queryId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            dispatchFuture.cancel(true);
            failQuery(queryId, new ErrorDetail(
                    "QUERY_CANCELLED", "ASYNC_SUBMIT_INTERRUPTED",
                    "Query submission was interrupted"), "async_submit_interrupted");
            throw new QueryRejectedException("Query submission was interrupted");
        }
        RuntimeException failure = dispatchFailure.get();
        if (failure != null) {
            throw failure;
        }
        var state = sm.getState();
        if (state == QueryStateMachine.State.FAILED
                || state == QueryStateMachine.State.CANCELLED) {
            dispatchFuture.cancel(true);
            String detail = sm.getError().map(ErrorDetail::message)
                    .orElse("Query ended before the result endpoint was ready");
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    detail);
        }
        if (state != QueryStateMachine.State.FAILED
                && state != QueryStateMachine.State.CANCELLED
                && (sm.getResultEndpoint().isEmpty()
                        || (state != QueryStateMachine.State.RUNNING
                                && state != QueryStateMachine.State.FINISHING
                                && state != QueryStateMachine.State.FINISHED))) {
            if (!sm.isTerminal()) {
                failQuery(queryId, new ErrorDetail(
                        "INTERNAL_ERROR", "RESULT_ENDPOINT_UNAVAILABLE",
                        "Query dispatch completed without a ready result endpoint"),
                        "async_result_endpoint_unavailable");
            }
            throw new IllegalStateException(
                    "Query dispatch completed without a ready result endpoint: " + queryId);
        }
        return buildResponse(queryId, sm, null, queryDispatcher.removeColumnNames(queryId));
    }

    /** Enriches after registration so concurrent submissions see the reserved query ID. */
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
        sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
        QueryStateMachine existing = queries.putIfAbsent(queryId, sm);
        if (existing != null) {
            // F-COORD-19: Reject concurrent submission of the same queryId while
            // the original is registered to prevent ownership and journal races.
            throw new QueryRejectedException(
                    "Query already registered: queryId=" + queryId
                    + " state=" + existing.getState());
        }
        // PERF-T17: Increment active count when a new query is registered.
        activeQueryCount.incrementAndGet();
        return sm;
    }

    private void registerJournalListener(String queryId, QueryStateMachine sm) {
        sm.addStateChangeListener((oldState, newState) -> {
            if (isIntermediateState(newState)) {
                stateJournal.appendQueryEvent(new QueryStateEvent(
                        queryId, null, null,
                        newState.name(), System.currentTimeMillis(),
                        localCoordinatorId));
            }
        });
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
        // The HTTP boundary never accepts a client-provided tenant. When tenant
        // isolation is enabled, resolve membership even for a missing identity
        // so the guard can deny it. A deployment without the tenant module has
        // one explicit default tenant rather than an absent tenant identity.
        String tenantId = tenantGuard != null
                ? tenantGuard.checkAndResolve(session.userIdentity())
                : (session.tenantId() != null && !session.tenantId().isBlank()
                        ? session.tenantId() : "default");
        return new CoordinatorSessionContext(
                session.queryId(), session.sessionProperties(),
                session.userIdentity(), tenantId);
    }

    private void checkDegradation() {
        if (resilienceRegistry == null) {
            return;
        }
        if (!resilienceRegistry.allowRequest("coordinator-query-admission")) {
            throw new QueryRejectedException(
                    "Query rejected: system overloaded — admission circuit breaker open");
        }
    }

    /**
     * Records a success or failure outcome to the query admission circuit breaker.
     * This feeds the breaker so it can open on sustained failures and reject new queries.
     */
    private void recordAdmissionOutcome(boolean success) {
        if (resilienceRegistry == null) {
            return;
        }
        if (success) {
            resilienceRegistry.recordSuccess("coordinator-query-admission");
        } else {
            resilienceRegistry.recordFailure("coordinator-query-admission",
                    new RuntimeException("query failed"));
        }
    }

    /**
     * P1-5b: Checks the cluster-wide admission controller (backed by heartbeat-fed
     * global tenant usage) and rejects if the tenant would exceed its budget. On
     * admission, registers a terminal-state listener that automatically releases
     * the reservation when the query finishes/fails/cancels.
     */
    private Runnable checkClusterAdmission(String queryId, String tenantId, QueryStateMachine sm) {
        var controller = this.clusterAdmissionController;
        if (controller == null || tenantId == null || tenantId.isBlank()) {
            return null;
        }
        var request = org.pangrova.spi.resource.admission.AdmissionRequest.of(
                tenantId, "default", queryId);
        var decision = controller.admit(request);
        if (!decision.isAdmitted()) {
            throw new QueryRejectedException(
                    "Cluster admission denied for tenant " + tenantId + ": " + decision.reason());
        }
        // Automatically release the admission slot when the query reaches any terminal state.
        sm.addStateChangeListener((oldState, newState) -> {
            if (sm.isTerminal()) {
                controller.release(queryId, tenantId);
            }
        });
        return () -> controller.release(queryId, tenantId);
    }

    /**
     * P1-5b: Sets the cluster-wide admission controller. When non-null, every query
     * submission must pass the controller's admission gate before execution. Wired
     * from {@code QueryBootstrap} when a {@code GlobalTenantUsageView} is available.
     */
    public void setClusterAdmissionController(
            org.pangrova.spi.resource.admission.ClusterAdmissionController controller) {
        this.clusterAdmissionController = controller;
        LOG.info("event=cluster_admission_controller_wired type={}",
                controller != null ? controller.getClass().getSimpleName() : "null");
    }

    private void checkThrottle(String queryId, CoordinatorSessionContext session) {
        if (throttlingService == null) {
            return;
        }
        String tenantId = session.tenantId() != null ? session.tenantId() : "default";
        String userId = session.userIdentity() != null
                ? session.userIdentity().principal() : "anonymous";
        var result = throttlingService.tryAcquire(tenantId, userId);
        if (!result.acquired()) {
            throw new QueryRejectedException(
                    "Query throttled: " + result.reason());
        }
        throttlePermits.put(queryId, result.permit());
    }

    /**
     * Releases the global throttle permit for the given query, if one was acquired.
     * Idempotent: safe to call multiple times for the same queryId.
     */
    private void releaseThrottlePermit(String queryId) {
        if (throttlingService == null) {
            return;
        }
        ThrottlePermit permit = throttlePermits.remove(queryId);
        if (permit != null) {
            throttlingService.release(permit);
        }
    }

    /** Attempts to cancel a query by transitioning to CANCELLED and cancelling dispatched tasks. */
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

    /**
     * A-XNODE-01: query-state lookup with journal fallback for Active-Active deployments.
     * When this coordinator has no local state machine (the query was accepted by a peer),
     * falls back to the shared {@link QueryStateJournal} so cross-node polls observe the
     * query's last journaled state instead of 404. Terminal states are purged from the
     * journal shortly after completion, so a miss here still means NOT_FOUND.
     */
    public Optional<QueryStateMachine.State> getQueryStateWithJournalFallback(String queryId) {
        Optional<QueryStateMachine.State> local = getQueryState(queryId);
        if (local.isPresent()) {
            return local;
        }
        return stateJournal.latestQueryEvent(queryId).flatMap(event -> {
            try {
                return Optional.of(QueryStateMachine.State.valueOf(event.state()));
            } catch (IllegalArgumentException e) {
                LOG.warn("event=journal_state_unparseable queryId={} state={}",
                        queryId, event.state());
                return Optional.empty();
            }
        });
    }

    /**
     * SI-6: wires this coordinator's node id for owner-stamped journal events.
     * Called once from bootstrap after the HA node id is known.
     */
    public void setLocalCoordinatorId(String coordinatorId) {
        this.localCoordinatorId = coordinatorId;
    }

    public Optional<ResultEndpoint> getResultEndpoint(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        return sm != null ? sm.getResultEndpoint() : Optional.empty();
    }

    public QueryStateMachine getStateMachine(String queryId) {
        return queries.get(queryId);
    }

    public void registerStateMachine(String queryId, QueryStateMachine sm) {
        synchronized (sm) {
            // Recovery may register an already-journaled terminal snapshot. It
            // has no local permit or active-count slot to release again.
            boolean recoveredTerminal = sm.isTerminal();
            sm.setBeforeTerminalTransition(() -> shuffleCleanupPending.accept(queryId));
            if (queries.putIfAbsent(queryId, sm) == null) {
                if (recoveredTerminal) {
                    sm.claimTerminalEffects();
                } else {
                    // PERF-T17: externally registered active queries own a slot.
                    activeQueryCount.incrementAndGet();
                }
            }
        }
    }

    public QueryStateJournal getStateJournal() {
        return stateJournal;
    }

    public int getActiveQueryCount() {
        // PERF-T17: O(1) read from atomic counter instead of O(N) stream().filter().
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

    /**
     * Blocks until all active queries reach terminal state or timeout expires.
     * Sets draining flag under write lock to block new submissions, then
     * waits on drainSignalLock condition for completion signals.
     */
    public boolean awaitAllQueries(long timeoutMs) {
        queryLifecycleLock.writeLock().lock();
        try {
            draining = true;
        } finally {
            queryLifecycleLock.writeLock().unlock();
        }
        drainSignalLock.lock();
        try {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (getActiveQueryCount() > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    allQueriesDrained.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            drainSignalLock.unlock();
        }
    }

    public Set<String> getActiveQueryIds() {
        return queries.entrySet().stream()
                .filter(e -> !e.getValue().isTerminal())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public Map<String, QueryStateMachine> getStateMachines() {
        return Map.copyOf(queries);
    }

    // --- Cleanup delegation ---

    public int cleanupTerminalQueries(Duration retention) {
        return cleanupScheduler.cleanupTerminalQueries(retention);
    }

    public void startPeriodicCleanup(ScheduledExecutorService executor,
                                     Duration retention, Duration interval) {
        cleanupScheduler.startPeriodicCleanup(executor, Runnable::run, retention, interval);
    }

    /** Runs potentially slow terminal cleanup on a bounded executor, not the timer thread. */
    public void startPeriodicCleanup(ScheduledExecutorService timer,
                                     Executor cleanupExecutor,
                                     Duration retention, Duration interval) {
        cleanupScheduler.startPeriodicCleanup(timer, cleanupExecutor, retention, interval);
    }

    public void stopPeriodicCleanup() {
        cleanupScheduler.stopPeriodicCleanup();
    }

    /**
     * Called when all tasks for a query have finished successfully.
     * Catches up intermediate states (SCHEDULING->RUNNING) to handle the race
     * where Worker completes before Coordinator transitions to RUNNING.
     */
    public void onAllTasksFinished(String queryId) {
        QueryStateMachine sm = queries.get(queryId);
        if (sm == null) {
            return;
        }
        if (sm.getState() == QueryStateMachine.State.FINISHED) {
            // A previous synchronous journal append may have failed after the
            // state transition; retry only the unapplied completion effects.
            completeFinishedQuery(queryId, sm);
            return;
        }
        // Catch up intermediate states for the race where Worker completes
        // before Coordinator transitions to RUNNING.
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        if (!sm.tryTransition(QueryStateMachine.State.FINISHED)) {
            // Another thread already moved to FAILED/CANCELLED — bail out
            // to prevent double cleanup and corrupted audit/lineage.
            return;
        }
        completeFinishedQuery(queryId, sm);
    }

    private void completeFinishedQuery(String queryId, QueryStateMachine sm) {
        completeTerminalQuery(queryId, sm, "all_tasks_finished");
    }

    private void completeTerminalQuery(String queryId, QueryStateMachine sm, String reason) {
        if (!sm.isTerminal()) {
            return;
        }
        TerminalCompletion completion = pendingTerminalCompletion.get(queryId);
        if (sm.terminalEffectsApplied() && completion == null) {
            return;
        }
        if (completion == null) {
            TerminalCompletion candidate = new TerminalCompletion(sm, reason);
            TerminalCompletion prior = pendingTerminalCompletion.putIfAbsent(queryId, candidate);
            completion = prior != null ? prior : candidate;
        }
        if (!completion.inProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!sm.terminalEffectsApplied()) {
                try {
                    stateJournal.appendQueryEventSync(new QueryStateEvent(
                            queryId, null, null, sm.getState().name(),
                            System.currentTimeMillis(), localCoordinatorId));
                } catch (RuntimeException journalFailure) {
                    scheduleTerminalRetry(queryId, completion);
                    throw journalFailure;
                }
                if (sm.claimTerminalEffects()) {
                    // These local releases precede potentially slow remote cleanup.
                    activeQueryCount.decrementAndGet();
                    releaseThrottlePermit(queryId);
                    signalDrainProgress();
                    applyTerminalObservability(queryId, sm, completion.reason);
                }
            }
            try {
                if (sm.getState() != QueryStateMachine.State.FINISHED) {
                    queryDispatcher.cancelDispatchedTasks(queryId);
                }
                queryDispatcher.cleanupQuery(queryId);
                pendingTerminalCompletion.remove(queryId, completion);
            } catch (RuntimeException cleanupFailure) {
                scheduleTerminalRetry(queryId, completion);
                LOG.warn("event=query_terminal_cleanup_pending queryId={} state={} error={}",
                        queryId, sm.getState(), cleanupFailure.getMessage(), cleanupFailure);
            }
        } finally {
            completion.inProgress.set(false);
        }
    }

    private void applyTerminalObservability(
            String queryId, QueryStateMachine sm, String reason) {
        runTerminalEffect(queryId, "journal_purge", () -> stateJournal.purgeQuery(queryId));
        if (sm.getState() == QueryStateMachine.State.FINISHED) {
            runTerminalEffect(queryId, "completed_metric", () -> incrementCounter(queryCompleteCounter));
            runTerminalEffect(queryId, "admission", () -> recordAdmissionOutcome(true));
            LOG.info("event=query_completed queryId={}", queryId);
            runTerminalEffect(queryId, "lineage", () -> auditBridge.emitLineageOnFinished(queryId));
            runTerminalEffect(queryId, "audit", () -> auditBridge.emitAuditCompleted(queryId, sm));
        } else {
            runTerminalEffect(queryId, "failed_metric", () -> incrementCounter(queryFailCounter));
            runTerminalEffect(queryId, "admission", () -> recordAdmissionOutcome(
                    sm.getState() == QueryStateMachine.State.CANCELLED));
            if (sm.getState() == QueryStateMachine.State.FAILED) {
                ErrorDetail error = sm.getError().orElse(null);
                String message = error != null ? error.message() : "unknown";
                LOG.warn("event=query_failed queryId={} reason={} trigger={}",
                        queryId, message, reason);
                runTerminalEffect(queryId, "audit", () -> auditBridge.emitAuditFailed(
                        queryId, sm, error));
            } else {
                LOG.info("event=query_cancelled queryId={}", queryId);
            }
        }
    }

    private void runTerminalEffect(String queryId, String effect, Runnable operation) {
        try {
            operation.run();
        } catch (RuntimeException failure) {
            LOG.warn("event=query_terminal_effect_failed queryId={} effect={} error={}",
                    queryId, effect, failure.getMessage(), failure);
        }
    }

    private void scheduleTerminalRetry(String queryId, TerminalCompletion completion) {
        synchronized (completion) {
            if (completion.retryQueued) {
                return;
            }
            completion.retryQueued = true;
            long delay = INITIAL_RETRY_DELAY_NANOS << completion.retryFailures;
            completion.retryFailures = Math.min(completion.retryFailures + 1, 8);
            delay = Math.min(delay, MAX_RETRY_DELAY_NANOS);
            synchronized (terminalRetryQueue) {
                terminalRetryQueue.add(new RetryTicket(
                        queryId, completion, nanoClock.getAsLong() + delay));
            }
        }
    }

    void retryPendingTerminalCompletions() {
        for (int attempted = 0; attempted < MAX_TERMINAL_RETRIES_PER_TICK;) {
            RetryTicket ticket;
            synchronized (terminalRetryQueue) {
                ticket = terminalRetryQueue.peek();
                if (ticket == null || ticket.dueAtNanos() - nanoClock.getAsLong() > 0) {
                    return;
                }
                terminalRetryQueue.remove();
            }
            TerminalCompletion completion = ticket.completion();
            synchronized (completion) {
                if (pendingTerminalCompletion.get(ticket.queryId()) != completion) {
                    continue;
                }
                completion.retryQueued = false;
            }
            attempted++;
            try {
                completeTerminalQuery(ticket.queryId(), completion.stateMachine,
                        completion.reason);
            } catch (RuntimeException failure) {
                LOG.warn("event=query_terminal_completion_retry_failed queryId={} error={}",
                        ticket.queryId(), failure.getMessage(), failure);
            }
        }
    }

    /**
     * Called when a task has failed. Transitions query to FAILED state.
     */
    public void onTaskFailed(String queryId, ErrorDetail error) {
        failQuery(queryId, error, "task_failed");
    }

    /**
     * Called when a query exceeds its timeout. Performs the same terminal
     * workflow as task failure — unified cleanup/audit/metrics path.
     */
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

    /** Emits a TABLE_ACCESSED audit event. Called from SqlCompiler access control path. */
    public void emitTableAccessedAudit(String queryId, String catalog, String schema,
                                       String table, CoordinatorSessionContext session) {
        auditBridge.emitTableAccessedAudit(queryId, catalog, schema, table, session);
    }

    private static String serializeSession(CoordinatorSessionContext session) {
        if (session == null) return null;
        return "{\"queryId\":\"" + session.queryId()
                + "\",\"tenantId\":\"" + (session.tenantId() != null ? session.tenantId() : "")
                + "\"}";
    }

    private static boolean isIntermediateState(QueryStateMachine.State state) {
        return state == QueryStateMachine.State.PLANNING
                || state == QueryStateMachine.State.SCHEDULING
                || state == QueryStateMachine.State.RUNNING
                || state == QueryStateMachine.State.FINISHING
                || state == QueryStateMachine.State.CANCELLED;
    }

    private static SubmitQueryResponse buildResponse(
            String queryId, QueryStateMachine sm, DispatchResult result, List<String> columnNames) {
        var endpoint = (result != null)
                ? result.resultEndpoint()
                : sm.getResultEndpoint().orElse(null);
        return new SubmitQueryResponse(
                queryId, sm.getState().name(), endpoint, columnNames);
    }

    /**
     * Signals the drain condition when a query reaches terminal state.
     * Uses drainSignalLock (not the lifecycle write lock) so signals are
     * never silently lost when readers hold the read lock (F-COORD-14).
     */
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
