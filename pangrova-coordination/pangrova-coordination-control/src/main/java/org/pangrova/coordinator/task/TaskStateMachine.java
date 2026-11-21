package org.pangrova.coordinator.task;

import org.pangrova.coordinator.enums.ScheduledTaskState;

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

    private static final Map<ScheduledTaskState, Set<ScheduledTaskState>> TRANSITIONS = Map.of(
            ScheduledTaskState.PENDING, Set.of(ScheduledTaskState.SCHEDULED),
            // A short task's exact FINISHED report can arrive before its RUNNING report.
            ScheduledTaskState.SCHEDULED, Set.of(ScheduledTaskState.RUNNING, ScheduledTaskState.FINISHED,
                    ScheduledTaskState.FAILED),
            ScheduledTaskState.RUNNING, Set.of(ScheduledTaskState.FINISHED, ScheduledTaskState.FAILED),
            ScheduledTaskState.FAILED, Set.of(ScheduledTaskState.ABANDONED),
            ScheduledTaskState.FINISHED, Set.of(),
            ScheduledTaskState.ABANDONED, Set.of()
    );

    private final AtomicReference<ScheduledTaskState> state = new AtomicReference<>(ScheduledTaskState.PENDING);
    private volatile String failureReason;
    // FINISHED alone is not a commit acknowledgement. Only the explicit
    // terminal signal settles this future after the owning commit boundary.
    private final CompletableFuture<Void> completionSignal = new CompletableFuture<>();

    CompletableFuture<Void> completionSignal() {
        return completionSignal;
    }

    public boolean tryTransition(ScheduledTaskState target) {
        while (true) {
            ScheduledTaskState current = state.get();
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

    public ScheduledTaskState getState() {
        return state.get();
    }

    public String getFailureReason() {
        return failureReason;
    }

    public boolean isTerminal() {
        ScheduledTaskState current = state.get();
        return current == ScheduledTaskState.FINISHED || current == ScheduledTaskState.FAILED
                || current == ScheduledTaskState.ABANDONED;
    }
}
