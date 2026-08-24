package org.pangrova.coordinator.task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * CAS-based state machine for coordinator-side task lifecycle.
 * Tracks retry count and failure reason across rescheduling cycles.
 *
 * <p>Concurrency strategy: AtomicReference for state, AtomicInteger for retry count,
 * volatile for failure reason. All transitions are CAS-based and lock-free.</p>
 */
@ThreadSafe
public class TaskStateMachine {

    public enum State {
        PENDING, SCHEDULED, RUNNING, FINISHED, FAILED, RESCHEDULING, ABANDONED
    }

    private static final Map<State, Set<State>> TRANSITIONS = Map.of(
            State.PENDING, Set.of(State.SCHEDULED),
            State.SCHEDULED, Set.of(State.RUNNING, State.FAILED),
            State.RUNNING, Set.of(State.FINISHED, State.FAILED),
            State.FAILED, Set.of(State.RESCHEDULING, State.ABANDONED),
            State.RESCHEDULING, Set.of(State.SCHEDULED),
            State.FINISHED, Set.of(),
            State.ABANDONED, Set.of()
    );

    private final AtomicReference<State> state = new AtomicReference<>(State.PENDING);
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private volatile String failureReason;

    public boolean tryTransition(State target) {
        while (true) {
            State current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(target)) {
                return false;
            }
            if (state.compareAndSet(current, target)) {
                if (current == State.FAILED && target == State.RESCHEDULING) {
                    retryCount.incrementAndGet();
                }
                return true;
            }
        }
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }

    public State getState() {
        return state.get();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public String getFailureReason() {
        return failureReason;
    }

    public boolean isTerminal() {
        return TRANSITIONS.getOrDefault(state.get(), Set.of()).isEmpty();
    }
}
