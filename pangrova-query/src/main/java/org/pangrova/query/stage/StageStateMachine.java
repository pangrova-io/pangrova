package org.pangrova.query.stage;

import org.pangrova.query.api.enums.StageState;

import org.pangrova.query.api.stage.StageStateListener;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

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

    private static final Map<StageState, Set<StageState>> TRANSITIONS = Map.of(
            StageState.PLANNED, Set.of(StageState.SCHEDULING, StageState.CANCELLED),
            StageState.SCHEDULING, Set.of(StageState.RUNNING, StageState.FAILED, StageState.CANCELLED),
            StageState.RUNNING, Set.of(StageState.FLUSHING, StageState.FAILED, StageState.CANCELLED),
            StageState.FLUSHING, Set.of(StageState.FINISHED, StageState.FAILED),
            StageState.FINISHED, Set.of(),
            StageState.FAILED, Set.of(),
            StageState.CANCELLED, Set.of()
    );

    private final String stageId;
    private final String queryId;
    private final AtomicReference<StageState> state = new AtomicReference<>(StageState.PLANNED);
    private final List<StageStateListener> listeners = new CopyOnWriteArrayList<>();

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
    public boolean transitionTo(StageState target) {
        checkNotNull(target, "target state must not be null");
        while (true) {
            StageState current = state.get();
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
    public boolean tryTransitionTo(StageState target) {
        checkNotNull(target, "target state must not be null");
        while (true) {
            StageState current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(target)) {
                return false;
            }
            if (state.compareAndSet(current, target)) {
                notifyListeners(current, target);
                return true;
            }
        }
    }

    public void addStateChangeListener(StageStateListener listener) {
        checkNotNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    public StageState getState() {
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
            StageState current = state.get();
            if (current != StageState.FAILED && current != StageState.CANCELLED) {
                return false; // completed output must never be reopened
            }
            if (state.compareAndSet(current, StageState.PLANNED)) {
                LOG.info("event=stage_state_reset stageId={} queryId={} from={}", stageId, queryId, current);
                return true;
            }
        }
    }

    private void notifyListeners(StageState oldState, StageState newState) {
        for (StageStateListener listener : listeners) {
            try {
                listener.onStateChange(stageId, oldState, newState);
            } catch (Exception e) {
                LOG.error("event=stage_listener_failed stageId={} queryId={} transition={}→{} error={}",
                        stageId, queryId, oldState, newState, e.getMessage(), e);
            }
        }
    }
}
