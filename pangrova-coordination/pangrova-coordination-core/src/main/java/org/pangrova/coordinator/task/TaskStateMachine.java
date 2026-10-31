package org.pangrova.coordinator.task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * CAS-based state machine for coordinator-side task lifecycle.
 * One physical attempt never returns to scheduling after a terminal outcome.
 *
 * <p>Concurrency strategy: AtomicReference for state and volatile failure reason.
 * All transitions are CAS-based and lock-free.</p>
 */
@ThreadSafe
public class TaskStateMachine {

    public enum State {
        PENDING, SCHEDULED, RUNNING, FINISHED, FAILED, ABANDONED
    }

    private static final Map<State, Set<State>> TRANSITIONS = Map.of(
            State.PENDING, Set.of(State.SCHEDULED),
            // A short task's exact FINISHED report can arrive before its RUNNING report.
            State.SCHEDULED, Set.of(State.RUNNING, State.FINISHED, State.FAILED),
            State.RUNNING, Set.of(State.FINISHED, State.FAILED),
            State.FAILED, Set.of(State.ABANDONED),
            State.FINISHED, Set.of(),
            State.ABANDONED, Set.of()
    );

    private final AtomicReference<State> state = new AtomicReference<>(State.PENDING);
    private volatile String failureReason;
    // FINISHED alone is not a commit acknowledgement. Only the explicit
    // terminal signal settles this future after the owning commit boundary.
    private final CompletableFuture<Void> completionSignal = new CompletableFuture<>();

    CompletableFuture<Void> completionSignal() {
        return completionSignal;
    }

    public boolean tryTransition(State target) {
        while (true) {
            State current = state.get();
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(target)) {
                return false;
            }
            if (state.compareAndSet(current, target)) {
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

    public String getFailureReason() {
        return failureReason;
    }

    public boolean isTerminal() {
        State current = state.get();
        return current == State.FINISHED || current == State.FAILED || current == State.ABANDONED;
    }
}
