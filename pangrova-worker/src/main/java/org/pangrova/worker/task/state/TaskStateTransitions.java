package org.pangrova.worker.task.state;

import org.pangrova.worker.enums.TaskState;

import java.util.Map;
import java.util.Set;

/** Defines the permitted state transitions for worker tasks. */
public final class TaskStateTransitions {

    private static final Map<TaskState, Set<TaskState>> TRANSITIONS = Map.ofEntries(
            Map.entry(TaskState.RECEIVED, Set.of(TaskState.INITIALIZING)),
            Map.entry(TaskState.INITIALIZING, Set.of(TaskState.RUNNING, TaskState.FAILED, TaskState.CANCELLING)),
            Map.entry(TaskState.RUNNING, Set.of(TaskState.FINISHING, TaskState.FAILED, TaskState.CANCELLING)),
            Map.entry(TaskState.FINISHING, Set.of(TaskState.FINISHED, TaskState.FAILED)),
            Map.entry(TaskState.CANCELLING, Set.of(TaskState.CANCELLED)),
            Map.entry(TaskState.FINISHED, Set.of()),
            Map.entry(TaskState.FAILED, Set.of()),
            Map.entry(TaskState.CANCELLED, Set.of()));

    private TaskStateTransitions() {
    }

    /** Returns whether the next state is a legal transition from the current state. */
    public static boolean isAllowed(TaskState current, TaskState next) {
        return TRANSITIONS.getOrDefault(current, Set.of()).contains(next);
    }
}
