package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.task.QueryStageObservations;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.stage.StageCompletionCallback.SuccessfulAttempt;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Correlates admitted reports and task registrations for one exact live DAG. */
public final class TaskTerminalRegistry {
    private final String queryId;
    private final Map<String, Slot> slots = new HashMap<>();
    private boolean closed;
    private volatile QueryStageObservations observations;

    public TaskTerminalRegistry(String queryId) {
        this.queryId = Objects.requireNonNull(queryId);
    }

    /** Associates diagnostics with this exact registered DAG. */
    public void observeTaskSelections(QueryStageObservations target) {
        observations = target;
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
        if (early != null) deliver(early, stage, taskId, generation);
    }

    /** First terminal fact wins. A stage callback may synchronously retry or retire this DAG. */
    public void onTerminal(String query, String taskId, String state, QueryFailure error) {
        onTerminal(query, taskId, state, error, null);
    }

    /** Records the elected physical attempt's summary with its first logical terminal event. */
    public void onTerminal(String query, String taskId, String state, QueryFailure error, BuildKeyRange range) {
        onTerminal(query, taskId, state, error, range, null);
    }

    public void onSuccessfulTerminal(String query, String logicalTaskId, SuccessfulAttempt attempt,
            BuildKeyRange range) {
        onTerminal(query, logicalTaskId, "FINISHED", null, range, attempt);
    }

    private void onTerminal(String query, String taskId, String state, QueryFailure error, BuildKeyRange range,
            SuccessfulAttempt attempt) {
        if (!queryId.equals(query) || !("FINISHED".equals(state) || "FAILED".equals(state)
                || "CANCELLED".equals(state))) {
            return;
        }
        Event event = new Event(state, error, range, attempt);
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
        if (stage != null) deliver(event, stage, taskId, generation);
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
        deliver(event, stage, taskId, generation);
    }

    private void deliver(Event event, Stage stage, String logicalTaskId, int generation) {
        var target = observations;
        if (event.deliver(stage, logicalTaskId, generation) && event.attempt() != null && target != null) {
            target.recordSelection(stage.stageId(), generation, logicalTaskId, event.attempt());
        }
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

    private record Event(String state, QueryFailure error, BuildKeyRange range, SuccessfulAttempt attempt) {
        private boolean deliver(Stage stage, String taskId, int generation) {
            if ("FINISHED".equals(state)) {
                return stage.onTaskFinished(taskId, generation, range);
            }
            QueryFailure cause = "CANCELLED".equals(state) && error == null
                    ? new QueryFailure("TASK_CANCELLED", "TaskCancelled", "Task was cancelled") : error;
            stage.onTaskFailed(taskId, cause, generation);
            return false;
        }
    }
}
