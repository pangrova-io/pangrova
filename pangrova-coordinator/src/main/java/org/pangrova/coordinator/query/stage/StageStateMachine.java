package org.pangrova.coordinator.query.stage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * CAS-based state machine for Stage lifecycle.
 * Terminal states: FINISHED, FAILED, CANCELLED.
 *
 * <p>Thread-safe: state transitions use CAS; listeners use CopyOnWriteArrayList.
 *
 * <p>Valid transitions:
 * <pre>
 * PLANNED → SCHEDULING → RUNNING → FLUSHING → FINISHED
 *                                            → FAILED
 *                                            → CANCELLED
 * PLANNED → CANCELLED
 * SCHEDULING → FAILED / CANCELLED
 * RUNNING → FAILED / CANCELLED
 * </pre>
 */
@ThreadSafe
public final class StageStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(StageStateMachine.class);

    public enum State {
        PLANNED, SCHEDULING, RUNNING, FLUSHING, FINISHED, FAILED, CANCELLED
    }

    private static final Map<State, Set<State>> TRANSITIONS = Map.of(
            State.PLANNED, Set.of(State.SCHEDULING, State.CANCELLED),
            State.SCHEDULING, Set.of(State.RUNNING, State.FAILED, State.CANCELLED),
            State.RUNNING, Set.of(State.FLUSHING, State.FAILED, State.CANCELLED),
            State.FLUSHING, Set.of(State.FINISHED, State.FAILED),
            State.FINISHED, Set.of(),
            State.FAILED, Set.of(),
            State.CANCELLED, Set.of()
    );

    private final String stageId;
    private final String queryId;
    private final AtomicReference<State> state = new AtomicReference<>(State.PLANNED);
    private final List<StateChangeListener> listeners = new CopyOnWriteArrayList<>();

    public StageStateMachine(String stageId, String queryId) {
        this.stageId = checkNotNull(stageId, "stageId must not be null");
        this.queryId = checkNotNull(queryId, "queryId must not be null");
    }

    /**
     * Attempts a CAS transition to the target state.
     * Notifies all listeners on success.
     *
     * @param target the desired target state
     * @return true if the transition succeeded
     * @throws IllegalStateException if the transition is not allowed
     */
    public boolean transitionTo(State target) {
        checkNotNull(target, "target state must not be null");
        while (true) {
            State current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(target)) {
                throw new IllegalStateException(
                        "Invalid transition: " + current + " → " + target
                                + " [stageId=" + stageId + ", queryId=" + queryId + "]");
            }
            if (state.compareAndSet(current, target)) {
                notifyListeners(current, target);
                return true;
            }
        }
    }

    /**
     * Attempts a CAS transition without throwing on illegal transitions.
     *
     * @param target the desired target state
     * @return true if the transition succeeded, false if not allowed
     */
    public boolean tryTransitionTo(State target) {
        checkNotNull(target, "target state must not be null");
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

    public void addStateChangeListener(StateChangeListener listener) {
        checkNotNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    public State getState() {
        return state.get();
    }

    public String getStageId() {
        return stageId;
    }

    public String getQueryId() {
        return queryId;
    }

    public boolean isTerminal() {
        return TRANSITIONS.getOrDefault(state.get(), Set.of()).isEmpty();
    }

    /**
     * Resets the state machine to PLANNED for stage retry.
     * Only valid from terminal states (FAILED, CANCELLED).
     * Callers must ensure no concurrent transitions are in progress.
     *
     * @return true if the reset succeeded
     */
    public boolean reset() {
        while (true) {
            State current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).isEmpty()) {
                return false; // not a terminal state
            }
            if (state.compareAndSet(current, State.PLANNED)) {
                LOG.info("event=stage_state_reset stageId={} queryId={} from={}", stageId, queryId, current);
                return true;
            }
        }
    }

    private void notifyListeners(State oldState, State newState) {
        for (StateChangeListener listener : listeners) {
            try {
                listener.onStateChange(stageId, oldState, newState);
            } catch (Exception e) {
                LOG.error("event=stage_listener_failed stageId={} queryId={} transition={}→{} error={}",
                        stageId, queryId, oldState, newState, e.getMessage(), e);
            }
        }
    }
}
