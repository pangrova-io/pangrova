package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

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
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.task.QueryIssuanceOperation;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.coordination.QueryStateEvent;

/**
 * CAS-based state machine for query lifecycle.
 * Terminal states: FINISHED, FAILED, CANCELLED.
 *
 * <p>Maintains a Stage state aggregation view for querying Stage-level progress.
 */
@ThreadSafe
public class QueryStateMachine {

    public enum State {
        QUEUED, PLANNING, SCHEDULING, RUNNING, FINISHING, FINISHED, FAILED, CANCELLED
    }

    private static final Map<State, Set<State>> TRANSITIONS = Map.of(
            State.QUEUED, Set.of(State.PLANNING, State.FAILED, State.CANCELLED),
            State.PLANNING, Set.of(State.SCHEDULING, State.FAILED, State.CANCELLED),
            State.SCHEDULING, Set.of(State.RUNNING, State.FAILED, State.CANCELLED),
            State.RUNNING, Set.of(State.FINISHING, State.FAILED, State.CANCELLED),
            State.FINISHING, Set.of(State.FINISHED, State.FAILED),
            State.FINISHED, Set.of(),
            State.FAILED, Set.of(),
            State.CANCELLED, Set.of()
    );

    private final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);
    private final Instant createdAt = Instant.now();
    private volatile String tenantId;
    private final String auditPrincipal;
    private final AtomicReference<ErrorDetail> error = new AtomicReference<>();
    private volatile ResultEndpoint resultEndpoint;
    private final CompletableFuture<ResultEndpoint> resultEndpointReady =
            new CompletableFuture<>();
    private final AtomicBoolean terminalEffectsApplied = new AtomicBoolean();
    private final AtomicBoolean physicalCleanupApplied = new AtomicBoolean();
    private final AtomicBoolean journalOwnership = new AtomicBoolean();
    private volatile Runnable admissionRelease;
    private final AtomicReference<org.pangrova.spi.coordination.QueryLineageFact> lineageStart =
            new AtomicReference<>();
    private final AtomicReference<org.pangrova.spi.coordination.QueryLineageFact> lineageTerminal =
            new AtomicReference<>();
    private org.pangrova.spi.execution.eventlistener.QueryLineageMetadata lineageMetadata =
            org.pangrova.spi.execution.eventlistener.QueryLineageMetadata.empty();

    /** Holds authorized plan lineage for the full query lifetime, independent of diagnostic cache eviction. */
    public synchronized void bindLineageMetadata(
            org.pangrova.spi.execution.eventlistener.QueryLineageMetadata metadata) {
        java.util.Objects.requireNonNull(metadata, "metadata");
        if (connectorAdmissionClosed() || lineageTerminal.get() != null) {
            throw new java.util.concurrent.CancellationException("Query lineage admission has closed");
        }
        lineageMetadata = metadata;
    }

    /** Returns the immutable authorized plan lineage retained by this query owner. */
    public synchronized org.pangrova.spi.execution.eventlistener.QueryLineageMetadata lineageMetadata() {
        return lineageMetadata;
    }

    synchronized org.pangrova.spi.coordination.QueryLineageFact freezeLineage(
            org.pangrova.spi.coordination.QueryLineageFact.Phase phase,
            java.util.function.Supplier<org.pangrova.spi.coordination.QueryLineageFact> create) {
        var slot = phase == org.pangrova.spi.coordination.QueryLineageFact.Phase.START
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

    org.pangrova.spi.coordination.QueryLineageFact lineageStart() { return lineageStart.get(); }


    void retainAdmissionRelease(Runnable release) { admissionRelease = release; }

    void releaseAdmission() {
        Runnable release = admissionRelease;
        if (release != null) {
            release.run();
            admissionRelease = null;
        }
    }

    boolean claimPhysicalCleanup() { return physicalCleanupApplied.compareAndSet(false, true); }
    boolean physicalCleanupApplied() { return physicalCleanupApplied.get(); }
    void retainJournalOwnership() { journalOwnership.set(true); }
    boolean hasJournalOwnership() { return journalOwnership.get(); }
    private final List<QueryStateChangeListener> listeners = new CopyOnWriteArrayList<>();
    private volatile Runnable beforeTerminalTransition = () -> { };
    private volatile java.util.function.BiConsumer<State, State> transitionRecorder;
    private volatile java.util.function.Supplier<QueryIssuanceOperation> transitionOperations;
    private final ConcurrentHashMap<State, QueryStateEvent> journalFacts = new ConcurrentHashMap<>();
    private final CompletableFuture<Boolean> ownershipReady;
    private final boolean locallySubmitted;
    /** A reserved terminal transition; waiters join only after leaving this monitor. */
    @javax.annotation.concurrent.GuardedBy("this")
    private CompletableFuture<Void> terminalTransitionPending;
    @javax.annotation.concurrent.GuardedBy("this")
    private State pendingTransitionTarget;
    @javax.annotation.concurrent.GuardedBy("this")
    private QueryConnectorAuthority connectorAuthority;

    /** Binds exactly once to this query's real issued scope, never to a later query-ID lookup. */
    public synchronized org.pangrova.spi.connector.ConnectorQueryAuthority bindConnectorAuthority(
            org.pangrova.spi.connector.ConnectorQueryAuthority issued) {
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
    public synchronized org.pangrova.spi.connector.ConnectorQueryAuthority connectorAuthority() {
        if (connectorAuthority == null) {
            throw new IllegalStateException("Query has no issued connector authority");
        }
        return connectorAuthority;
    }

    synchronized org.pangrova.spi.connector.ConnectorQueryOperation enterConnectorOperation(
            org.pangrova.spi.connector.ConnectorQueryAuthority issued) {
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

    QueryStateMachine(boolean ownershipPending) {
        this(ownershipPending, null);
    }

    QueryStateMachine(boolean ownershipPending, CoordinatorSessionContext session) {
        locallySubmitted = ownershipPending;
        auditPrincipal = session != null && session.userIdentity() != null
                ? session.userIdentity().principal() : "anonymous";
        ownershipReady = ownershipPending
                ? new CompletableFuture<>() : CompletableFuture.completedFuture(true);
    }

    void resolveOwnership(boolean acquired) {
        ownershipReady.complete(acquired);
    }

    boolean awaitOwnership() {
        return ownershipReady.join();
    }

    boolean discardUnadmittedQuery() { return locallySubmitted && !hasJournalOwnership(); }

    /** Stage state aggregation: stageId → current StageStateMachine.State. */
    private final ConcurrentHashMap<String, StageStateMachine.State> stageStates =
            new ConcurrentHashMap<>();

    /**
     * Callback for query state transitions.
     * Implementations must be thread-safe and non-blocking.
     */
    @FunctionalInterface
    public interface QueryStateChangeListener {
        void onStateChange(State oldState, State newState);
    }

    /** Registers a listener notified on every successful state transition. */
    public void addStateChangeListener(QueryStateChangeListener listener) {
        listeners.add(listener);
    }

    /** Attempts an atomic transition to the target state; returns false when not allowed. */
    public boolean tryTransition(State target) {
        return tryTransition(target, null);
    }

    /** Publishes the winning failure's error with its terminal state. */
    boolean tryTransitionToFailed(ErrorDetail failureError) {
        return tryTransition(State.FAILED, failureError);
    }

    private boolean tryTransition(State target, ErrorDetail failureError) {
        if (!TRANSITIONS.getOrDefault(state.get(), Set.of()).contains(target)) return false;
        boolean terminal = TRANSITIONS.getOrDefault(target, Set.of()).isEmpty();
        boolean executionOwned = !terminal || awaitOwnership();
        if (!executionOwned && target != State.FAILED) return false;
        org.pangrova.coordinator.task.QueryIssuanceOperation operation;
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

    private synchronized Transition reserveTransition(State target) {
        if (terminalTransitionPending != null) return new Transition(null, terminalTransitionPending);
        State previous = state.get();
        if (!TRANSITIONS.getOrDefault(previous, Set.of()).contains(target)) return null;
        var pending = new CompletableFuture<Void>();
        Transition decision = new Transition(previous, pending);
        terminalTransitionPending = pending;
        pendingTransitionTarget = target;
        return decision;
    }

    private void publishTransition(Transition decision, State target, ErrorDetail failureError,
            boolean executionOwned) {
        try {
            if (executionOwned && TRANSITIONS.getOrDefault(target, Set.of()).isEmpty()) {
                beforeTerminalTransition.run();
            }
            java.util.function.BiConsumer<State, State> recorder = transitionRecorder;
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
            if (target == State.FAILED && failureError != null) error.compareAndSet(null, failureError);
            state.set(target);
            terminalTransitionPending = null;
            pendingTransitionTarget = null;
        }
        decision.pending().complete(null);
    }

    private record Transition(State previous, CompletableFuture<Void> pending) { }

    void setTransitionRecorder(java.util.function.BiConsumer<State, State> recorder) {
        transitionRecorder = java.util.Objects.requireNonNull(recorder);
    }

    void setTransitionOperations(java.util.function.Supplier<QueryIssuanceOperation> operations) {
        transitionOperations = java.util.Objects.requireNonNull(operations);
    }

    org.pangrova.spi.coordination.QueryStateEvent journalEvent(State target,
            java.util.function.Supplier<org.pangrova.spi.coordination.QueryStateEvent> create) {
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

    public State getState() {
        return state.get();
    }

    public boolean isTerminal() {
        return TRANSITIONS.getOrDefault(state.get(), Set.of()).isEmpty();
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


    public Optional<ErrorDetail> getError() {
        return Optional.ofNullable(error.get());
    }

    /**
     * Sets the error detail. Uses {@code compareAndSet(null, error)} to ensure
     * only the first error is recorded — subsequent calls are no-ops. This
     * prevents a later error from overwriting the original failure trigger
     * when multiple failure paths race (e.g., stage failure + timeout).
     */
    public void setError(ErrorDetail errorDetail) {
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
        State current = state.get();
        if (endpoint != null && (current == State.RUNNING
                || current == State.FINISHING || current == State.FINISHED)) {
            resultEndpointReady.complete(endpoint);
        }
    }

    CompletableFuture<ResultEndpoint> resultEndpointReady() {
        return resultEndpointReady;
    }

    boolean claimTerminalEffects() {
        return terminalEffectsApplied.compareAndSet(false, true);
    }

    boolean terminalEffectsApplied() {
        return terminalEffectsApplied.get();
    }

    // ── Stage state aggregation ──

    /**
     * Updates the tracked state for a Stage.
     * Called by StageStateMachine listeners to maintain the aggregation view.
     */
    public void updateStageState(String stageId, StageStateMachine.State stageState) {
        stageStates.put(stageId, stageState);
    }

    /**
     * Returns the current state of a specific Stage, or empty if not tracked.
     */
    public Optional<StageStateMachine.State> getStageState(String stageId) {
        return Optional.ofNullable(stageStates.get(stageId));
    }

    /**
     * Returns an immutable snapshot of all Stage states.
     */
    public Map<String, StageStateMachine.State> getStageStates() {
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
    public void onStageFailed(String stageId, ErrorDetail stageError) {
        updateStageState(stageId, StageStateMachine.State.FAILED);
        tryTransitionToFailed(stageError);
    }

    private void notifyListeners(State oldState, State newState) {
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
            if (getState() == State.FINISHED) {
                return;
            }
            // Walk through intermediate states using the TRANSITIONS map.
            // Each tryTransition is a no-op if the current state cannot reach
            // the target, so this safely advances from any non-terminal state.
            for (State step : List.of(State.PLANNING, State.SCHEDULING,
                    State.RUNNING, State.FINISHING, State.FINISHED)) {
                if (getState() == State.FINISHED) {
                    return;
                }
                tryTransition(step);
            }
            State current = getState();
            if (current != State.FINISHED) {
                throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                        "Cannot transition query to FINISHED — current state: " + current);
            }
        };
    }
}
