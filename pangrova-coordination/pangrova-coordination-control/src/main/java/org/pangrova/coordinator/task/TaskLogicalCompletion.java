package org.pangrova.coordinator.task;

import org.pangrova.coordinator.enums.ScheduledTaskState;

import java.util.concurrent.CompletableFuture;

/** Worker-state interpretation and logical waiter completion; neither is physical-drain evidence. */
final class TaskLogicalCompletion {
    private TaskLogicalCompletion() {}

    static void complete(String taskId, TaskStateMachine sm, TaskTimingTracker timingTracker) {
        ScheduledTaskState state = sm.getState();
        CompletableFuture<Void> future = sm.completionSignal();
        if (state == ScheduledTaskState.FINISHED) {
            timingTracker.removeDeadline(taskId);
            future.complete(null);
        } else if (state == ScheduledTaskState.FAILED
                || state == ScheduledTaskState.ABANDONED) {
            timingTracker.removeDeadline(taskId);
            future.completeExceptionally(
                    new IllegalStateException("Task terminated: " + taskId
                            + ", state=" + state));
        }
    }

    static ScheduledTaskState mapWorkerState(String workerState) {
        return switch (workerState) {
            case "RUNNING", "FLUSHING" -> ScheduledTaskState.RUNNING;
            case "FINISHED" -> ScheduledTaskState.FINISHED;
            case "FAILED" -> ScheduledTaskState.FAILED;
            case "CANCELLED" -> ScheduledTaskState.FAILED;
            case "SCHEDULED" -> ScheduledTaskState.SCHEDULED;
            case "ABANDONED" -> ScheduledTaskState.ABANDONED;
            default -> throw new IllegalArgumentException("Unknown worker state: " + workerState);
        };
    }
}

