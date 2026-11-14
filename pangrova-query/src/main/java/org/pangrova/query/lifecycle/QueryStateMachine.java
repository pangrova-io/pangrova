package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.enums.StageState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.lifecycle.QueryOperation;
import org.pangrova.query.api.error.QueryFailure;
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

    private final AtomicReference<QueryState> state = new AtomicReference<>(QueryState.QUEUED);
    private final Instant createdAt = Instant.now();
    private volatile String tenantId;
    private final String auditPrincipal;
    private volatile String sql;
    private volatile org.pangrova.common.client.query.BranchSelection branchSelection;

    public void selectBranch(org.pangrova.common.client.query.BranchSelection selection) {
        this.branchSelection = java.util.Objects.requireNonNull(selection);
    }

    public org.pangrova.common.client.query.BranchSelection branchSelection() {
        return branchSelection;
    }
    private final AtomicReference<QueryFailure> error = new AtomicReference<>();
    private volatile ResultEndpoint resultEndpoint;
    private final CompletableFuture<ResultEndpoint> resultEndpointReady =
            new CompletableFuture<>();
    private final CompletableFuture<Void> writeDispatchReady = new CompletableFuture<>();
    private final CompletableFuture<QueryState> terminalStateReady = new CompletableFuture<>();
    private final CompletableFuture<QueryState> nativeWriteCompletionReady = new CompletableFuture<>();
    private volatile boolean controlPlaneWriteFinalization;
    private final AtomicBoolean terminalEffectsApplied = new AtomicBoolean();
    private final AtomicBoolean physicalCleanupApplied = new AtomicBoolean();
    private final AtomicBoolean journalOwnership = new AtomicBoolean();
    private volatile Runnable admissionRelease;
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
            org.pangrova.query.api.lineage.QueryLineageFact.Phase phase,
            java.util.function.Supplier<org.pangrova.query.api.lineage.QueryLineageFact> create) {
        var slot = phase == org.pangrova.query.api.lineage.QueryLineageFact.Phase.START
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

    /** Serialized admission/cleanup owner releases the reservation; failure retains it for retry. */
    public void releaseAdmission() {
        Runnable release = admissionRelease;
        if (release != null) {
            release.run();
            admissionRelease = null;
        }
    }

    /** Claims bookkeeping once, only after the cleanup owner has completed physical release. */
    public boolean claimPhysicalCleanup() { return physicalCleanupApplied.compareAndSet(false, true); }
    /** Logical termination alone never sets this physical completion evidence. */
    public boolean physicalCleanupApplied() { return physicalCleanupApplied.get(); }
    /** Journal owner records an accepted admission, including an outcome requiring confirmation. */
    public void retainJournalOwnership() { journalOwnership.set(true); }
    /** Whether this query retains responsibility for a potentially accepted journal admission. */
    public boolean hasJournalOwnership() { return journalOwnership.get(); }
    private final List<QueryStateChangeListener> listeners = new CopyOnWriteArrayList<>();
    private volatile Runnable beforeTerminalTransition = () -> { };
    private volatile java.util.function.BiConsumer<QueryState, QueryState> transitionRecorder;
    private volatile java.util.function.Supplier<QueryOperation> transitionOperations;
    private final ConcurrentHashMap<QueryState, QueryStateEvent> journalFacts = new ConcurrentHashMap<>();
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
    public void bindSql(String sql) {
        this.sql = sql;
    }

    /** Submitted SQL text, or null when the query was rejected before parsing. */
    public String getSql() {
        return sql;
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
        if (!TRANSITIONS.getOrDefault(state.get(), Set.of()).contains(target)) return false;
        boolean terminal = TRANSITIONS.getOrDefault(target, Set.of()).isEmpty();
        boolean executionOwned = !terminal || awaitOwnership();
        if (!executionOwned && target != QueryState.FAILED) return false;
        org.pangrova.query.api.lifecycle.QueryOperation operation;
        try {
            operation = transitionOperations == null ? null : transitionOperations.get();
        } catch (IllegalStateException closedAdmission) {
            if (!TRANSITIONS.getOrDefault(state.get(), Set.of()).contains(target)) return false;
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
        QueryState previous = state.get();
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
            java.util.function.BiConsumer<QueryState, QueryState> recorder = transitionRecorder;
            if (recorder != null) recorder.accept(decision.previous(), target);
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                terminalTransitionPending = null;
                pendingTransitionTarget = null;
            }
            decision.pending().complete(null);
            throw failure;
        }
        synchronized (this) {
            if (target == QueryState.FAILED && failureError != null) error.compareAndSet(null, failureError);
            state.set(target);
            terminalTransitionPending = null;
            pendingTransitionTarget = null;
        }
        decision.pending().complete(null);
        if (TRANSITIONS.getOrDefault(target, Set.of()).isEmpty()) {
            terminalStateReady.complete(target);
            nativeWriteCompletionReady.complete(target);
        }
    }

    private record Transition(QueryState previous, CompletableFuture<Void> pending) { }

    /** Journal owner installs recording before publication; recording failure prevents the transition. */
    public void setTransitionRecorder(java.util.function.BiConsumer<QueryState, QueryState> recorder) {
        transitionRecorder = java.util.Objects.requireNonNull(recorder);
    }

    /** Execution owner installs the permit supplier before publication; each transition closes its permit. */
    public void setTransitionOperations(java.util.function.Supplier<QueryOperation> operations) {
        transitionOperations = java.util.Objects.requireNonNull(operations);
    }

    /** Retains one immutable event per target state for exact journal retries. */
    public org.pangrova.query.api.journal.QueryStateEvent journalEvent(QueryState target,
            java.util.function.Supplier<org.pangrova.query.api.journal.QueryStateEvent> create) {
        var existing = journalFacts.get(target);
        if (existing != null) return existing;
        var candidate = create.get();
        var retained = journalFacts.putIfAbsent(target, candidate);
        return retained != null ? retained : candidate;
    }

    /** Persists terminal cleanup responsibility before a terminal state becomes visible. */
    public synchronized void setBeforeTerminalTransition(Runnable recorder) {
        beforeTerminalTransition = java.util.Objects.requireNonNull(recorder, "recorder");
    }

    public QueryState getState() {
        return state.get();
    }

    public boolean isTerminal() {
        return TRANSITIONS.getOrDefault(state.get(), Set.of()).isEmpty();
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

    /** Sets the tenant identifier for per-tenant quota filtering. */
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /** Returns elapsed time since query creation in milliseconds. */
    public long getDurationMs() {
        return java.time.Duration.between(createdAt, Instant.now()).toMillis();
    }

    public Optional<QueryFailure> getError() {
        return Optional.ofNullable(error.get());
    }

    /**
     * Sets the error detail. Uses {@code compareAndSet(null, error)} to ensure
     * only the first error is recorded — subsequent calls are no-ops. This
     * prevents a later error from overwriting the original failure trigger
     * when multiple failure paths race (e.g., stage failure + timeout).
     */
    public void setError(QueryFailure errorDetail) {
        error.compareAndSet(null, errorDetail);
    }

    public Optional<ResultEndpoint> getResultEndpoint() {
        return Optional.ofNullable(resultEndpoint);
    }

    public void setResultEndpoint(ResultEndpoint resultEndpoint) {
        this.resultEndpoint = java.util.Objects.requireNonNull(
                resultEndpoint, "resultEndpoint must not be null");
        completeResultEndpointReadiness();
    }

    private void completeResultEndpointReadiness() {
        ResultEndpoint endpoint = resultEndpoint;
        QueryState current = state.get();
        if (endpoint != null && (current == QueryState.RUNNING
                || current == QueryState.FINISHING || current == QueryState.FINISHED)) {
            resultEndpointReady.complete(endpoint);
        }
    }

    /** Observer-owned wait; completing or cancelling it cannot alter the owner's readiness. */
    public CompletableFuture<ResultEndpoint> resultEndpointReady() {
        return resultEndpointReady.copy();
    }

    /** Signals that a resultless write has reached its actual Worker dispatch. */
    public void markWriteDispatchReady() {
        QueryState current = state.get();
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
