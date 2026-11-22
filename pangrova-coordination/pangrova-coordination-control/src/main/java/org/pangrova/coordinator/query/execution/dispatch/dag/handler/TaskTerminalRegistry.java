package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Correlates admitted reports and task registrations for one exact live DAG. */
public final class TaskTerminalRegistry {
    private final String queryId;
    private final Map<String, Slot> slots = new HashMap<>();
    private boolean closed;

    public TaskTerminalRegistry(String queryId) {
        this.queryId = Objects.requireNonNull(queryId);
    }

    /** Captures the owning generation and delivers at most one buffered terminal event. */
    public void register(String query, String taskId, Stage stage) {
        if (!queryId.equals(query) || !queryId.equals(stage.stateMachine().getQueryId())) {
            throw new IllegalArgumentException("Task registration differs from its owning query");
        }
        int generation = stage.currentGeneration();
        Event early;
        synchronized (this) {
            if (closed) return;
            Slot slot = slots.computeIfAbsent(taskId, ignored -> new Slot());
            if (slot.stage != null) {
                if (slot.stage != stage || slot.generation != generation) {
                    throw new IllegalStateException("Task identity already belongs to another stage attempt: "
                            + taskId);
                }
                return;
            }
            slot.stage = stage;
            slot.generation = generation;
            early = slot.event;
        }
        if (early != null) early.deliver(stage, taskId, generation);
    }

    /** First terminal fact wins. A stage callback may synchronously retry or retire this DAG. */
    public void onTerminal(String query, String taskId, String state, QueryFailure error) {
        onTerminal(query, taskId, state, error, null);
    }

    /** Records the elected physical attempt's summary with its first logical terminal event. */
    public void onTerminal(String query, String taskId, String state, QueryFailure error, BuildKeyRange range) {
        if (!queryId.equals(query) || !("FINISHED".equals(state) || "FAILED".equals(state)
                || "CANCELLED".equals(state))) {
            return;
        }
        Event event = new Event(state, error, range);
        Stage stage;
        int generation;
        synchronized (this) {
            if (closed) return;
            Slot slot = slots.computeIfAbsent(taskId, ignored -> new Slot());
            if (slot.event != null) return;
            slot.event = event;
            stage = slot.stage;
            generation = slot.generation;
        }
        if (stage != null) event.deliver(stage, taskId, generation);
    }

    /** A failed replacement restored the old task after its first terminal report may have been routed. */
    public void replayRestoredTask(String taskId, Stage stage) {
        Event event;
        int generation;
        synchronized (this) {
            Slot slot = slots.get(taskId);
            if (closed || slot == null || slot.stage != stage || slot.event == null) return;
            event = slot.event;
            generation = slot.generation;
        }
        event.deliver(stage, taskId, generation);
    }

    /** Closes admission even to registrations that acquired this registry before retirement. */
    public synchronized void close() {
        closed = true;
        slots.clear();
    }

    private static final class Slot {
        private Stage stage;
        private int generation;
        private Event event;
    }

    private record Event(String state, QueryFailure error, BuildKeyRange range) {
        private void deliver(Stage stage, String taskId, int generation) {
            if ("FINISHED".equals(state)) {
                stage.onTaskFinished(taskId, generation, range);
            } else {
                QueryFailure cause = "CANCELLED".equals(state) && error == null
                        ? new QueryFailure("TASK_CANCELLED", "TaskCancelled", "Task was cancelled") : error;
                stage.onTaskFailed(taskId, cause, generation);
            }
        }
    }
}
