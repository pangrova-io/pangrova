package org.pangrova.coordinator.query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;

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
    private final AtomicReference<ErrorDetail> error = new AtomicReference<>();
    private volatile ResultEndpoint resultEndpoint;
    private final List<QueryStateChangeListener> listeners = new CopyOnWriteArrayList<>();

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
        while (true) {
            State current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(target)) {
                return false;
            }
            if (state.compareAndSet(current, target)) {
                notifyListeners(current, target);
                return true;
            }
        }
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
        this.resultEndpoint = resultEndpoint;
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
        if (stageError != null) {
            error.compareAndSet(null, stageError);
        }
        tryTransition(State.FAILED);
    }

    private void notifyListeners(State oldState, State newState) {
        for (QueryStateChangeListener listener : listeners) {
            listener.onStateChange(oldState, newState);
        }
    }

    /**
     * Creates a {@link CoordinatorResultService.QueryFinisher} that advances
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
    public CoordinatorResultService.QueryFinisher toFinisher() {
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

