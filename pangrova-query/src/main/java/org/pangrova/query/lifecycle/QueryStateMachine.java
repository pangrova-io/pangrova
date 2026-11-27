package org.pangrova.query.lifecycle;

import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.enums.StageState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.lifecycle.QueryOperation;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.query.api.journal.QueryStateEvent;

/**
 * Single state-transition owner for query lifecycle.
 * Terminal states: FINISHED, FAILED, CANCELLED.
 *
 * <p>Reserves transitions under its monitor, performs journal/fencing callbacks outside
 * that monitor, then publishes state and readiness. Connector operation admission uses
 * the same reservation boundary. Failed durable recording does not publish a terminal state.
 *
 * <p>Internal lifecycle collaborators install callbacks before query publication and
 * record cleanup only after the corresponding physical owner has released resources.
 * Public visibility permits cross-module engine use; this is not a provider SPI or a
 * second journal authority. Maintains a Stage state aggregation view for progress queries.
 */
@ThreadSafe
@org.pangrova.common.api.annotation.Internal
public class QueryStateMachine {

    private static final Map<QueryState, Set<QueryState>> TRANSITIONS = Map.of(
            QueryState.QUEUED, Set.of(QueryState.PLANNING, QueryState.FAILED, QueryState.CANCELLED),
            QueryState.PLANNING, Set.of(QueryState.SCHEDULING, QueryState.FAILED, QueryState.CANCELLED),
            QueryState.SCHEDULING, Set.of(QueryState.RUNNING, QueryState.FAILED, QueryState.CANCELLED),
            QueryState.RUNNING, Set.of(QueryState.FINISHING, QueryState.FAILED, QueryState.CANCELLED),
            QueryState.FINISHING, Set.of(QueryState.FINISHED, QueryState.FAILED),
            QueryState.FINISHED, Set.of(),
            QueryState.FAILED, Set.of(),
            QueryState.CANCELLED, Set.of()
    );

    private volatile Status status =
            new Status(0, QueryState.QUEUED, null, ResultDisposition.PENDING, null, false, null);
    private Map<QueryWaitTarget, Set<CompletableFuture<Status>>> statusWaiters;
    private final Instant createdAt = Instant.now();
    private volatile String tenantId;
    private final String auditPrincipal;
    private volatile String sql;
    private volatile QueryAuditRecord terminalAudit;
    private volatile org.pangrova.query.api.session.BranchSelection branchSelection;

    public void selectBranch(org.pangrova.query.api.session.BranchSelection selection) {
        this.branchSelection = java.util.Objects.requireNonNull(selection);
    }

    public org.pangrova.query.api.session.BranchSelection branchSelection() {
        return branchSelection;
    }
    private final CompletableFuture<ResultEndpoint> resultEndpointReady =
            new CompletableFuture<>();
    private final CompletableFuture<Void> writeDispatchReady = new CompletableFuture<>();
    private final CompletableFuture<QueryState> terminalStateReady = new CompletableFuture<>();
    private final CompletableFuture<QueryState> nativeWriteCompletionReady = new CompletableFuture<>();
    private volatile boolean controlPlaneWriteFinalization;
    private volatile boolean readOnlyTaskExecution;
    private final AtomicBoolean terminalEffectsApplied = new AtomicBoolean();
    private final AtomicBoolean retirementCheckInProgress = new AtomicBoolean();
    private volatile boolean physicalCleanupApplied;
    private final AtomicBoolean journalOwnership = new AtomicBoolean();
    private volatile Runnable admissionRelease;
    @GuardedBy("this")
    private boolean admissionHealthProbe;
    private final AtomicReference<org.pangrova.query.api.lineage.QueryLineageFact> lineageStart =
            new AtomicReference<>();
    private final AtomicReference<org.pangrova.query.api.lineage.QueryLineageFact> lineageTerminal =
            new AtomicReference<>();
    private org.pangrova.query.api.lineage.QueryLineageMetadata lineageMetadata =
            org.pangrova.query.api.lineage.QueryLineageMetadata.empty();

    /** Holds authorized plan lineage for the full query lifetime, independent of diagnostic cache eviction. */
    public synchronized void bindLineageMetadata(
            org.pangrova.query.api.lineage.QueryLineageMetadata metadata) {
        java.util.Objects.requireNonNull(metadata, "metadata");
        if (connectorAdmissionClosed() || lineageTerminal.get() != null) {
            throw new java.util.concurrent.CancellationException("Query lineage admission has closed");
        }
        lineageMetadata = metadata;
    }

    /** Returns the immutable authorized plan lineage retained by this query owner. */
    public synchronized org.pangrova.query.api.lineage.QueryLineageMetadata lineageMetadata() {
        return lineageMetadata;
    }

    /** Retains the first exact fact for a phase so journal and audit retries share its identity. */
    public synchronized org.pangrova.query.api.lineage.QueryLineageFact freezeLineage(
            LineageEventType phase,
            java.util.function.Supplier<org.pangrova.query.api.lineage.QueryLineageFact> create) {
        var slot = phase == LineageEventType.START
                ? lineageStart : lineageTerminal;
        var retained = slot.get();
        if (retained == null) {
            var candidate = java.util.Objects.requireNonNull(create.get(), "lineage fact");
            if (candidate.phase() != phase) throw new IllegalArgumentException("Lineage phase changed");
            slot.compareAndSet(null, candidate);
            retained = slot.get();
        }
        if (retained.phase() != phase) throw new IllegalStateException("Query has a different frozen terminal fact");
        return retained;
    }

    /** Returns the frozen submission fact, or null before the submission owner creates it. */
    public org.pangrova.query.api.lineage.QueryLineageFact lineageStart() { return lineageStart.get(); }

    /** Admission owner installs its exact release obligation before attempting acquisition. */
    public void retainAdmissionRelease(Runnable release) { admissionRelease = release; }

    /** Clears the tentative release after admission confirms this request owns no reservation. */
    public void discardUnacquiredAdmission() { admissionRelease = null; }

    /** Serialized admission/cleanup owner releases the reservation; failure retains it for retry. */
    public void releaseAdmission() {
        Runnable release = admissionRelease;
        if (release != null) {
            release.run();
            admissionRelease = null;
        }
    }

    /** Completes local cleanup once and publishes drain immediately when retirement also committed. */
    public boolean claimPhysicalCleanup(boolean retired) {
        StatusChange change = StatusChange.UNOBSERVED;
        synchronized (this) {
            if (physicalCleanupApplied) return false;
            if (retired) change = publishPhysicalRetirement();
            physicalCleanupApplied = true;
        }
        change.notifyWaiters();
        return true;
    }

    /** Marks global drain after the recovery transaction returned the execution reservation. */
    public void confirmPhysicalRetirement() {
        StatusChange change;
        synchronized (this) {
            if (!physicalCleanupApplied || status.drained()) return;
            change = publishPhysicalRetirement();
        }
        change.notifyWaiters();
    }

    /** Coalesces concurrent demand checks after local cleanup has handed off retirement. */
    public void refreshPhysicalRetirement(java.util.function.BooleanSupplier retired) {
        if (!physicalCleanupApplied || status.drained() || !retirementCheckInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!status.drained() && retired.getAsBoolean()) confirmPhysicalRetirement();
        } finally {
            retirementCheckInProgress.set(false);
        }
    }

    private StatusChange publishPhysicalRetirement() {
        var previous = status;
        return publishStatus(previous.state(), previous.error(), previous.disposition(), previous.endpoint(), true);
    }

    /** Local ownership has drained, irrespective of a pending durable recovery. */
    public boolean physicalCleanupApplied() { return physicalCleanupApplied; }
    /** Journal owner records an accepted admission, including an outcome requiring confirmation. */
    public void retainJournalOwnership() { journalOwnership.set(true); }
    /** Whether this query retains responsibility for a potentially accepted journal admission. */
    public boolean hasJournalOwnership() { return journalOwnership.get(); }
    private volatile ExecutionAnchor journalGrant;

    /** Retains the immutable original grant for subsequent lifecycle recording and journal retirement. */
    public void bindJournalGrant(ExecutionAnchor grant) { journalGrant = grant; }

    public ExecutionAnchor journalGrant() { return journalGrant; }

    private final List<QueryStateChangeListener> listeners = new CopyOnWriteArrayList<>();
    private volatile Runnable beforeTerminalTransition = () -> { };
    private volatile java.util.function.Supplier<QueryOperation> transitionOperations;
    private volatile QueryStateEvent admissionJournalFact;
    private volatile QueryStateEvent terminalJournalFact;
    private final CompletableFuture<Boolean> ownershipReady;
    private final boolean locallySubmitted;
    /** A reserved terminal transition; waiters join only after leaving this monitor. */
    @org.pangrova.common.concurrent.GuardedBy("this")
    private CompletableFuture<Void> terminalTransitionPending;
    @org.pangrova.common.concurrent.GuardedBy("this")
    private QueryState pendingTransitionTarget;
    @org.pangrova.common.concurrent.GuardedBy("this")
    private QueryConnectorAuthority connectorAuthority;

    /** Binds exactly once to this query's real issued scope, never to a later query-ID lookup. */
    public synchronized org.pangrova.connector.api.metadata.ConnectorMetadataScope bindConnectorAuthority(
            org.pangrova.connector.api.query.ConnectorQueryAuthority issued) {
        java.util.Objects.requireNonNull(issued, "issued");
        if (connectorAdmissionClosed() || issued.isClosed()) {
            throw new IllegalStateException("Connector query admission is closed: " + issued.queryId());
        }
        if (connectorAuthority == null) connectorAuthority = new QueryConnectorAuthority(this, issued);
        if (!connectorAuthority.matches(issued)) {
            throw new IllegalStateException("Query issued connector authority changed");
        }
        return connectorAuthority;
    }

    synchronized boolean connectorAdmissionClosed() {
        return isTerminal() || pendingTransitionTarget != null
                && TRANSITIONS.get(pendingTransitionTarget).isEmpty();
    }

    /** Returns the exact connector lifetime bound during engine query creation. */
    public synchronized org.pangrova.connector.api.metadata.ConnectorMetadataScope connectorAuthority() {
        if (connectorAuthority == null) {
            throw new IllegalStateException("Query has no issued connector authority");
        }
        return connectorAuthority;
    }

    public synchronized boolean hasConnectorAuthority() {
        return connectorAuthority != null;
    }

    synchronized org.pangrova.connector.api.query.ConnectorQueryOperation enterConnectorOperation(
            org.pangrova.connector.api.query.ConnectorQueryAuthority issued) {
        if (connectorAdmissionClosed()) {
            throw new IllegalStateException("Connector query admission is closed: " + issued.queryId());
        }
        // Lock order is query state, then issued scope. Neither boundary performs I/O.
        return issued.enterOperation();
    }

    /** Creates a state machine whose ownership has already been established externally. */
    public QueryStateMachine() {
        this(false);
    }

    /** Creates an explicitly pending admission, or an externally established execution owner. */
    public QueryStateMachine(boolean ownershipPending) {
        this(ownershipPending, "anonymous");
    }

    /** Captures audit attribution before publication; it never creates authenticated identity. */
    public QueryStateMachine(boolean ownershipPending, String auditPrincipal) {
        locallySubmitted = ownershipPending;
        this.auditPrincipal = auditPrincipal;
        ownershipReady = ownershipPending
                ? new CompletableFuture<>() : CompletableFuture.completedFuture(true);
    }

    /** Submission owner resolves its admission outcome once; later completions cannot overwrite it. */
    public void resolveOwnership(boolean acquired) {
        ownershipReady.complete(acquired);
    }

    /** Waits outside the state monitor for submission admission, not for physical task drain. */
    public boolean awaitOwnership() {
        return ownershipReady.join();
    }

    /** Whether a local rejected submission can be removed without discarding journal responsibility. */
    public boolean discardUnadmittedQuery() { return locallySubmitted && !hasJournalOwnership(); }

    /** Binds the submitted SQL text before publication so terminal audit records are self-describing. */
    public void bindSql(String submittedSql) {
        this.sql = submittedSql;
    }

    /** Submitted SQL text, or null when the query was rejected before parsing. */
    public String getSql() {
        return sql;
    }

    /** Freezes the same audit record for local observation and durable persistence. */
    public synchronized QueryAuditRecord freezeTerminalAudit(String queryId) {
        if (!isTerminal()) {
            throw new IllegalStateException("Query audit requires a terminal state");
        }
        if (terminalAudit == null) {
            Instant completedAt = Instant.now();
            terminalAudit = QueryAuditRecord.wallTimeOnly(queryId, auditPrincipal, sql, tenantId,
                    java.time.Duration.between(createdAt, completedAt).toMillis(), completedAt,
                    status.state() == QueryState.FINISHED);
        }
        return terminalAudit;
    }

    /** The retained terminal audit record, if its query has published it. */
    public Optional<QueryAuditRecord> terminalAudit() {
        return Optional.ofNullable(terminalAudit);
    }

    /** Stage state aggregation: stageId → current StageState. */
    private final ConcurrentHashMap<String, StageState> stageStates =
            new ConcurrentHashMap<>();

    /**
     * Callback for query state transitions.
     * Implementations must be thread-safe and non-blocking.
     */
    @FunctionalInterface
    public interface QueryStateChangeListener {
        void onStateChange(QueryState oldState, QueryState newState);
    }

    /** Registers a listener notified on every successful state transition. */
    public void addStateChangeListener(QueryStateChangeListener listener) {
        listeners.add(listener);
    }

    /** Attempts an atomic transition to the target state; returns false when not allowed. */
    public boolean tryTransition(QueryState target) {
        return tryTransition(target, null);
    }

    /** Publishes the winning failure's error with its terminal state. */
    public boolean tryTransitionToFailed(QueryFailure failureError) {
        return tryTransition(QueryState.FAILED, failureError);
    }

    private boolean tryTransition(QueryState target, QueryFailure failureError) {
        if (!TRANSITIONS.getOrDefault(status.state(), Set.of()).contains(target)) return false;
        boolean terminal = TRANSITIONS.getOrDefault(target, Set.of()).isEmpty();
        boolean executionOwned = !terminal || awaitOwnership();
        if (!executionOwned && target != QueryState.FAILED) return false;
        org.pangrova.query.api.lifecycle.QueryOperation operation;
        try {
            operation = transitionOperations == null ? null : transitionOperations.get();
        } catch (IllegalStateException closedAdmission) {
            if (!TRANSITIONS.getOrDefault(status.state(), Set.of()).contains(target)) return false;
            throw closedAdmission;
        }
        try {
            while (true) {
                Transition decision = reserveTransition(target);
                if (decision == null) return false;
                if (decision.previous() == null) {
                    decision.pending().join();
                    continue;
                }
                publishTransition(decision, target, failureError, executionOwned);
                completeResultEndpointReadiness();
                notifyListeners(decision.previous(), target);
                return true;
            }
        } finally {
            if (operation != null) operation.close();
        }
    }

    private synchronized Transition reserveTransition(QueryState target) {
        if (terminalTransitionPending != null) return new Transition(null, terminalTransitionPending);
        QueryState previous = status.state();
        if (!TRANSITIONS.getOrDefault(previous, Set.of()).contains(target)) return null;
        var pending = new CompletableFuture<Void>();
        Transition decision = new Transition(previous, pending);
        terminalTransitionPending = pending;
        pendingTransitionTarget = target;
        return decision;
    }

    private void publishTransition(Transition decision, QueryState target, QueryFailure failureError,
            boolean executionOwned) {
        try {
            if (executionOwned && TRANSITIONS.getOrDefault(target, Set.of()).isEmpty()) {
                beforeTerminalTransition.run();
            }
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                terminalTransitionPending = null;
                pendingTransitionTarget = null;
            }
            decision.pending().complete(null);
            throw failure;
        }
        StatusChange change;
        synchronized (this) {
            Status previous = status;
            QueryFailure failure = previous.error();
            if (target == QueryState.FAILED && failure == null) failure = failureError;
            change = publishStatus(target, failure, previous.disposition(),
                    previous.endpoint(), previous.drained());
            terminalTransitionPending = null;
            pendingTransitionTarget = null;
        }
        decision.pending().complete(null);
        change.notifyWaiters();
        if (TRANSITIONS.getOrDefault(target, Set.of()).isEmpty()) {
            terminalStateReady.complete(target);
            nativeWriteCompletionReady.complete(target);
        }
    }

    private record Transition(QueryState previous, CompletableFuture<Void> pending) { }

    /** Execution owner installs the permit supplier before publication; each transition closes its permit. */
    public void setTransitionOperations(java.util.function.Supplier<QueryOperation> operations) {
        transitionOperations = java.util.Objects.requireNonNull(operations);
    }

    /** Retains only the immutable admission and terminal facts for exact journal retries. */
    public QueryStateEvent journalEvent(QueryState target, java.util.function.Supplier<QueryStateEvent> create) {
        boolean admission = target == QueryState.QUEUED;
        if (!admission && !TRANSITIONS.getOrDefault(target, Set.of()).isEmpty()) {
            throw new IllegalArgumentException("Query progress belongs to the live projection");
        }
        var existing = admission ? admissionJournalFact : terminalJournalFact;
        if (existing != null) return existing;
        var candidate = create.get();
        synchronized (this) {
            if (admission) {
                if (admissionJournalFact == null) admissionJournalFact = candidate;
                return admissionJournalFact;
            }
            if (terminalJournalFact == null) terminalJournalFact = candidate;
            return terminalJournalFact;
        }
    }

    /** Persists terminal cleanup responsibility before a terminal state becomes visible. */
    public synchronized void setBeforeTerminalTransition(Runnable recorder) {
        beforeTerminalTransition = java.util.Objects.requireNonNull(recorder, "recorder");
    }

    public QueryState getState() {
        return status.state();
    }

    public boolean isTerminal() {
        return TRANSITIONS.getOrDefault(status.state(), Set.of()).isEmpty();
    }

    /** Completes when the logical query, including its write commit, reaches a terminal state. */
    public CompletableFuture<QueryState> terminalStateReady() {
        return terminalStateReady.copy();
    }

    /** Holds query termination while a Native write is followed by control-plane publication. */
    public synchronized void deferTerminalUntilControlPlaneResult() {
        if (controlPlaneWriteFinalization || isTerminal()) {
            throw new IllegalStateException(
                    "Control-plane write finalization cannot be installed twice or after termination");
        }
        controlPlaneWriteFinalization = true;
    }

    /** Completes Native-write waiters without exposing FINISHED before publication. */
    public boolean signalNativeWriteCompletedForControlPlane() {
        if (!controlPlaneWriteFinalization) {
            return false;
        }
        QueryState current = getState();
        nativeWriteCompletionReady.complete(current == QueryState.FAILED || current == QueryState.CANCELLED
                ? current : QueryState.FINISHED);
        return true;
    }

    /** Waits for Native commit for a held control-plane write, or ordinary query termination. */
    public CompletableFuture<QueryState> nativeWriteCompletionReady() {
        return controlPlaneWriteFinalization ? nativeWriteCompletionReady.copy() : terminalStateReady();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    /** Returns the tenant identifier associated with this query, or null if unset. */
    public String getTenantId() {
        return tenantId;
    }

    /** Returns the submitting principal captured before this query becomes externally visible. */
    public String getAuditPrincipal() {
        return auditPrincipal;
    }

    /** Compares the exact submitting principal with an authenticated caller. */
    public boolean isOwnedBy(String principal) {
        return principal != null && !principal.isBlank() && principal.equals(auditPrincipal);
    }

    /** Sets the tenant identifier for per-tenant quota filtering. */
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /** Returns elapsed time since query creation in milliseconds. */
    public long getDurationMs() {
        return java.time.Duration.between(createdAt, Instant.now()).toMillis();
    }

    public Optional<QueryFailure> getError() {
        return Optional.ofNullable(status.error());
    }

    /**
     * Records the first error detail when multiple failure paths race.
     */
    public void setError(QueryFailure errorDetail) {
        if (errorDetail == null) return;
        StatusChange change;
        synchronized (this) {
            Status previous = status;
            if (previous.error() != null) return;
            change = publishStatus(previous.state(), errorDetail, previous.disposition(),
                    previous.endpoint(), previous.drained());
        }
        change.notifyWaiters();
    }

    public Optional<ResultEndpoint> getResultEndpoint() {
        return Optional.ofNullable(status.endpoint());
    }

    /** Publishes changed result authority in the same status version as the current query state. */
    public void setResultEndpoint(ResultEndpoint resultEndpoint) {
        java.util.Objects.requireNonNull(resultEndpoint, "resultEndpoint must not be null");
        StatusChange change;
        synchronized (this) {
            Status previous = status;
            if (resultEndpoint.equals(previous.endpoint())) return;
            if (previous.disposition() == ResultDisposition.NONE) {
                throw new IllegalStateException("A resultless query cannot publish a result endpoint");
            }
            change = publishStatus(previous.state(), previous.error(), ResultDisposition.STREAM,
                    resultEndpoint, previous.drained());
        }
        completeResultEndpointReadiness();
        change.notifyWaiters();
    }

    /** Publishes that this query intentionally has no client result stream. */
    public void markNoResult() {
        StatusChange change;
        synchronized (this) {
            Status previous = status;
            if (previous.disposition() == ResultDisposition.NONE) {
                return;
            }
            if (previous.disposition() == ResultDisposition.STREAM) {
                throw new IllegalStateException("A streaming query cannot discard its result endpoint");
            }
            change = publishStatus(previous.state(), previous.error(), ResultDisposition.NONE,
                    null, previous.drained());
        }
        change.notifyWaiters();
    }

    private void completeResultEndpointReadiness() {
        Status snapshot = status;
        QueryState current = snapshot.state();
        if (snapshot.disposition() == ResultDisposition.STREAM
                && (current == QueryState.RUNNING
                || current == QueryState.FINISHING || current == QueryState.FINISHED)) {
            resultEndpointReady.complete(snapshot.endpoint());
        }
    }

    /** Coherent state, result disposition, authority and physical-release evidence for one query owner. */
    public record Status(long version, QueryState state, QueryFailure error,
                         ResultDisposition disposition, ResultEndpoint endpoint, boolean drained,
                         org.pangrova.query.api.session.BranchSelection branchSelection) {
        public Status {
            if (disposition == null || disposition == ResultDisposition.UNAVAILABLE) {
                throw new IllegalArgumentException("Live status requires a live result disposition");
            }
            disposition.validateEndpoint(endpoint);
            if (disposition == ResultDisposition.PENDING && TRANSITIONS.get(state).isEmpty()) {
                throw new IllegalArgumentException("PENDING must not survive terminal completion");
            }
        }

        /** Checks whether this owner projection can satisfy the client's requested continuation. */
        public boolean reaches(QueryWaitTarget target, long observedVersion) {
            boolean terminal = TRANSITIONS.get(state).isEmpty();
            return switch (target) {
                case NEXT_VERSION -> version != observedVersion;
                case RESULT_OR_TERMINAL -> terminal
                        || (disposition == ResultDisposition.STREAM
                            && (state == QueryState.RUNNING || state == QueryState.FINISHING));
                case TERMINAL -> terminal;
                case DRAINED -> drained;
            };
        }
    }

    public Status status() {
        return status;
    }

    /** Admits a detachable observer in the same section that compares its version. */
    public synchronized CompletableFuture<Status> awaitStatus(QueryWaitTarget target, long observedVersion) {
        Status current = status;
        if (current.reaches(target, observedVersion)) return CompletableFuture.completedFuture(current);
        var observer = new CompletableFuture<Status>();
        if (statusWaiters == null) {
            statusWaiters = new EnumMap<>(QueryWaitTarget.class);
        }
        statusWaiters.computeIfAbsent(target, ignored -> new HashSet<>()).add(observer);
        observer.whenComplete((result, failure) -> removeStatusWaiter(target, observer));
        return observer;
    }

    private synchronized void removeStatusWaiter(QueryWaitTarget target, CompletableFuture<Status> observer) {
        if (statusWaiters == null) return;
        var waiting = statusWaiters.get(target);
        if (waiting != null && waiting.remove(observer) && waiting.isEmpty()) statusWaiters.remove(target);
        if (statusWaiters.isEmpty()) statusWaiters = null;
    }

    private StatusChange publishStatus(QueryState state, QueryFailure error,
            ResultDisposition disposition, ResultEndpoint endpoint, boolean drained) {
        long previousVersion = status.version();
        ResultDisposition publishedDisposition =
                TRANSITIONS.get(state).isEmpty() && disposition == ResultDisposition.PENDING
                        ? ResultDisposition.NONE
                        : disposition;
        var next = new Status(Math.incrementExact(previousVersion), state, error,
                publishedDisposition, endpoint, drained, state == QueryState.FINISHED ? branchSelection : null);
        status = next;
        if (statusWaiters == null) return StatusChange.UNOBSERVED;
        List<Set<CompletableFuture<Status>>> released = null;
        var waiting = statusWaiters.entrySet().iterator();
        while (waiting.hasNext()) {
            var entry = waiting.next();
            if (next.reaches(entry.getKey(), previousVersion)) {
                if (released == null) released = new ArrayList<>(statusWaiters.size());
                released.add(entry.getValue());
                waiting.remove();
            }
        }
        if (statusWaiters.isEmpty()) statusWaiters = null;
        return released == null ? StatusChange.UNOBSERVED : new StatusChange(released, next);
    }

    private record StatusChange(List<Set<CompletableFuture<Status>>> waiters, Status status) {
        private static final StatusChange UNOBSERVED = new StatusChange(List.of(), null);

        void notifyWaiters() {
            for (var group : waiters) {
                for (var observer : group) observer.complete(status);
            }
        }
    }

    /** Observer-owned wait; completing or cancelling it cannot alter the owner's readiness. */
    public CompletableFuture<ResultEndpoint> resultEndpointReady() {
        return resultEndpointReady.copy();
    }

    /** Signals that a resultless write has reached its actual Worker dispatch. */
    public void markWriteDispatchReady() {
        QueryState current = status.state();
        if (current == QueryState.RUNNING || current == QueryState.FINISHING || current == QueryState.FINISHED) {
            writeDispatchReady.complete(null);
        }
    }

    /** Observer-owned wait for actual Worker dispatch, independent of other observers' cancellation. */
    public CompletableFuture<Void> writeDispatchReady() {
        return writeDispatchReady.copy();
    }

    /** True only after the execution owner signals a dispatched write in an executable state. */
    public boolean isWriteDispatchReady() {
        return writeDispatchReady.isDone();
    }

    /** Records the scheduler's read-only execution before the first task can report. */
    public void markReadOnlyTaskExecution() {
        readOnlyTaskExecution = true;
    }

    /** Requires positive local planning evidence. Recovered executions have no such evidence. */
    public boolean hasReadOnlyTaskExecution() {
        return readOnlyTaskExecution;
    }

    /** Acquires a health probe before dispatch can outlive this local admission. */
    public synchronized void acquireAdmissionHealthProbe(java.util.function.BooleanSupplier acquire) {
        if (isTerminal()) {
            throw new QueryRejectedException("Query ended before admission");
        }
        if (admissionHealthProbe) {
            throw new IllegalStateException("Query already owns an admission health probe");
        }
        admissionHealthProbe = acquire.getAsBoolean();
    }

    /** Transfers this query's health probe to its terminal observer. */
    public synchronized boolean takeAdmissionHealthProbe() {
        boolean acquired = admissionHealthProbe;
        admissionHealthProbe = false;
        return acquired;
    }

    /** Terminal owner claims logical notifications once, after durable terminal recording succeeds. */
    public boolean claimTerminalEffects() {
        return terminalEffectsApplied.compareAndSet(false, true);
    }

    /** Logical publication evidence; does not certify physical resource release. */
    public boolean terminalEffectsApplied() {
        return terminalEffectsApplied.get();
    }

    // ── Stage state aggregation ──

    /**
     * Updates the tracked state for a Stage.
     * Called by StageStateMachine listeners to maintain the aggregation view.
     */
    public void updateStageState(String stageId, StageState stageState) {
        stageStates.put(stageId, stageState);
    }

    /**
     * Returns the current state of a specific Stage, or empty if not tracked.
     */
    public Optional<StageState> getStageState(String stageId) {
        return Optional.ofNullable(stageStates.get(stageId));
    }

    /**
     * Returns an immutable snapshot of all Stage states.
     */
    public Map<String, StageState> getStageStates() {
        return Map.copyOf(stageStates);
    }

    /**
     * Called when a Stage fails. Transitions the query to FAILED if not already terminal.
     *
     * <p>Sets the error atomically: the error is set BEFORE the CAS transition
     * so that any thread observing state=FAILED will also see the error detail.
     * Uses {@code compareAndSet(null, stageError)} to preserve the first error
     * when multiple stages fail concurrently.</p>
     */
    public void onStageFailed(String stageId, QueryFailure stageError) {
        updateStageState(stageId, StageState.FAILED);
        tryTransitionToFailed(stageError);
    }

    private void notifyListeners(QueryState oldState, QueryState newState) {
        for (QueryStateChangeListener listener : listeners) {
            listener.onStateChange(oldState, newState);
        }
    }

    /**
     * Creates a callback that advances
     * this state machine to FINISHED from any non-terminal state.
     *
     * <p>Walks through the state sequence (PLANNING → SCHEDULING → RUNNING →
     * FINISHING → FINISHED), attempting each transition via {@link #tryTransition}
     * which silently skips states not reachable from the current state. For example,
     * if the state machine is already in RUNNING, the PLANNING/SCHEDULING transitions
     * return false (no-op), and the finisher proceeds from RUNNING → FINISHING → FINISHED.</p>
     *
     * <p>Idempotent: if the state machine is already FINISHED, this is a no-op.
     * This is required because DML dispatch (DELETE/UPDATE/MERGE) may complete the
     * state machine internally via the DAG engine dispatch path, and the caller
     * (StatementDispatcher.toDispatchResult) invokes toFinisher a second time.</p>
     *
     * @throws PangrovaException if the state machine cannot reach FINISHED (e.g., FAILED/CANCELLED)
     */
    public Runnable toFinisher() {
        return () -> {
            if (getState() == QueryState.FINISHED) {
                return;
            }
            // Walk through intermediate states using the TRANSITIONS map.
            // Each tryTransition is a no-op if the current state cannot reach
            // the target, so this safely advances from any non-terminal state.
            for (QueryState step : List.of(QueryState.PLANNING, QueryState.SCHEDULING,
                    QueryState.RUNNING, QueryState.FINISHING, QueryState.FINISHED)) {
                if (getState() == QueryState.FINISHED) {
                    return;
                }
                tryTransition(step);
            }
            QueryState current = getState();
            if (current != QueryState.FINISHED) {
                throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                        "Cannot transition query to FINISHED — current state: " + current);
            }
        };
    }
}
