package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Atomically correlates task registration with asynchronously arriving terminal states. */
public final class TaskTerminalRegistry {
    private static final Logger LOG =
            LoggerFactory.getLogger(TaskTerminalRegistry.class);

    private final TaskFaultHandler taskFaultHandler = new TaskFaultHandler();
    private final ConcurrentHashMap<String, TaskSlot> taskSlots =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> queryTaskIndex =
            new ConcurrentHashMap<>();

    /** Registers a task and replays a terminal event that arrived first. */
    public void register(String queryId, String taskId, Stage stage) {
        queryTaskIndex.computeIfAbsent(
                queryId, ignored -> ConcurrentHashMap.newKeySet()).add(taskId);
        EarlyTerminalEvent[] replay = new EarlyTerminalEvent[1];
        taskSlots.compute(taskId, (key, existing) ->
                registerSlot(stage, existing, replay));
        LOG.info("event=task_registered queryId={} taskId={} stageState={}",
                queryId, taskId, stage.stateMachine().getState());
        replayEarlyTerminal(queryId, taskId, stage, replay[0]);
    }

    /** Routes or buffers a task terminal state. */
    public void onTerminal(
            String queryId,
            String taskId,
            String state,
            ErrorDetail error) {
        Stage[] route = new Stage[1];
        taskSlots.compute(taskId, (key, existing) ->
                terminalSlot(queryId, state, error, existing, route));
        if (route[0] != null) {
            routeTerminal(route[0], taskId, queryId, state, error);
            return;
        }
        LOG.info("event=early_terminal_buffered queryId={} taskId={} state={}",
                queryId, taskId, state);
    }

    /** Clears all terminal correlation state for a query. */
    public void clear(String queryId) {
        taskFaultHandler.clearQuery(queryId);
        Set<String> taskIds = queryTaskIndex.remove(queryId);
        if (taskIds != null) {
            taskIds.forEach(taskSlots::remove);
        }
    }

    private static TaskSlot registerSlot(
            Stage stage,
            TaskSlot existing,
            EarlyTerminalEvent[] replay) {
        if (existing != null && existing.stage() == null) {
            replay[0] = existing.earlyEvent();
            return new TaskSlot(stage, null, true);
        }
        return new TaskSlot(stage, null, false);
    }

    private static TaskSlot terminalSlot(
            String queryId,
            String state,
            ErrorDetail error,
            TaskSlot existing,
            Stage[] route) {
        if (existing == null || existing.stage() == null) {
            return new TaskSlot(
                    null, new EarlyTerminalEvent(queryId, state, error), false);
        }
        if (!existing.replayed()) {
            route[0] = existing.stage();
        }
        return existing;
    }

    private void replayEarlyTerminal(
            String queryId,
            String taskId,
            Stage stage,
            EarlyTerminalEvent event) {
        if (event == null) {
            return;
        }
        LOG.info("event=early_terminal_replayed queryId={} taskId={} state={}",
                queryId, taskId, event.state());
        taskSlots.remove(taskId);
        routeTerminal(stage, taskId, queryId, event.state(), event.error());
    }

    private void routeTerminal(
            Stage stage,
            String taskId,
            String queryId,
            String state,
            ErrorDetail error) {
        switch (state) {
            case "FINISHED" -> stage.onTaskFinished(taskId);
            case "FAILED" -> routeFailure(stage, taskId, queryId, error);
            case "CANCELLED" -> stage.onTaskFailed(
                    taskId, cancellationError(error));
            default -> {
            }
        }
        LOG.debug("event=task_terminal_routed queryId={} taskId={} state={}"
                        + " stageId={}",
                queryId, taskId, state, stage.stageId());
    }

    private void routeFailure(
            Stage stage, String taskId, String queryId, ErrorDetail error) {
        if (taskFaultHandler.isShuffleFetchFailure(error)) {
            LOG.info("event=shuffle_fetch_failure_detected queryId={}"
                            + " taskId={} stageId={}",
                    queryId, taskId, stage.stageId());
        }
        stage.onTaskFailed(taskId, error);
    }

    private static ErrorDetail cancellationError(ErrorDetail error) {
        return error != null ? error
                : new ErrorDetail(
                        "TASK_CANCELLED", "TaskCancelled", "Task was cancelled");
    }

    private record EarlyTerminalEvent(
            String queryId, String state, ErrorDetail error) {}

    private record TaskSlot(
            Stage stage, EarlyTerminalEvent earlyEvent, boolean replayed) {}
}
