package org.pangrova.coordinator.task;

import java.util.concurrent.CompletableFuture;

/** Worker-state interpretation and logical waiter completion; neither is physical-drain evidence. */
final class TaskLogicalCompletion {
    private TaskLogicalCompletion() {}

    static void complete(String taskId, TaskStateMachine sm, TaskTimingTracker timingTracker) {
        TaskStateMachine.State state = sm.getState();
        CompletableFuture<Void> future = sm.completionSignal();
        if (state == TaskStateMachine.State.FINISHED) {
            timingTracker.removeDeadline(taskId);
            future.complete(null);
        } else if (state == TaskStateMachine.State.FAILED
                || state == TaskStateMachine.State.ABANDONED) {
            timingTracker.removeDeadline(taskId);
            future.completeExceptionally(
                    new IllegalStateException("Task terminated: " + taskId
                            + ", state=" + state));
        }
    }

    static TaskStateMachine.State mapWorkerState(String workerState) {
        return switch (workerState) {
            case "RUNNING", "FLUSHING" -> TaskStateMachine.State.RUNNING;
            case "FINISHED" -> TaskStateMachine.State.FINISHED;
            case "FAILED" -> TaskStateMachine.State.FAILED;
            case "CANCELLED" -> TaskStateMachine.State.FAILED;
            case "SCHEDULED" -> TaskStateMachine.State.SCHEDULED;
            case "ABANDONED" -> TaskStateMachine.State.ABANDONED;
            default -> throw new IllegalArgumentException("Unknown worker state: " + workerState);
        };
    }
}

