package org.pangrova.coordinator.task;

import org.pangrova.coordinator.enums.ScheduledTaskState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.scheduler.api.speculation.TaskDuration;

/**
 * Tracks per-task deadlines and execution durations.
 *
 * <p>Owns deadline enforcement (timeout detection) and duration measurement
 * (straggler detection). The parent {@link CoordinatorTaskTracker} delegates
 * timing-related operations to this class.</p>
 *
 * <p>Concurrency strategy: all maps are ConcurrentHashMap for lock-free
 * concurrent access. Individual operations are atomic at the map level.</p>
 */
@ThreadSafe
public final class TaskTimingTracker {

    private final ConcurrentHashMap<String, Instant> taskDeadlines =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> taskQueryMapping =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Instant> taskStartTimes =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Instant> taskFinishTimes =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> taskStageMapping =
            new ConcurrentHashMap<>();

    /**
     * Records the query association for a task (used by deadline timeout resolution).
     *
     * @param taskId  the task identifier
     * @param queryId the query identifier
     */
    public void registerTaskQuery(String taskId, String queryId) {
        taskQueryMapping.put(taskId, queryId);
    }

    /** Returns the queryId associated with a taskId, or null if not registered. */
    public String getQueryIdForTask(String taskId) {
        return taskQueryMapping.get(taskId);
    }

    /**
     * Records a deadline for a dispatched task.
     *
     * @param taskId   the task identifier
     * @param deadline the absolute instant after which the task is considered timed-out
     */
    public void setDeadline(String taskId, Instant deadline) {
        taskDeadlines.put(taskId, deadline);
    }

    /**
     * Returns non-terminal tasks whose deadline has passed.
     *
     * @param now              the reference instant
     * @param nonTerminalCheck predicate that returns true if the task is still running
     * @return taskId → queryId for every task past its deadline and still running
     */
    public Map<String, String> getTimedOutTasks(Instant now,
                                                 java.util.function.BiPredicate<String, String> nonTerminalCheck) {
        Map<String, String> timedOut = new HashMap<>();
        for (Map.Entry<String, Instant> entry : taskDeadlines.entrySet()) {
            if (now.isBefore(entry.getValue())) {
                continue;
            }
            String taskId = entry.getKey();
            String queryId = taskQueryMapping.get(taskId);
            if (queryId != null && nonTerminalCheck.test(queryId, taskId)) {
                timedOut.put(taskId, queryId);
            }
        }
        return timedOut;
    }

    /** Removes deadline tracking for a task. */
    public void removeDeadline(String taskId) {
        taskDeadlines.remove(taskId);
    }

    /**
     * Records the start time for a task entering RUNNING state.
     *
     * @param taskId  the task identifier
     * @param stageId the stage identifier for grouping
     */
    public void recordTaskStart(String taskId, String stageId) {
        taskStartTimes.putIfAbsent(taskId, Instant.now());
        taskStageMapping.put(taskId, stageId);
    }

    /**
     * Records task start with an explicit start time (for testing or replay).
     */
    public void recordTaskStart(String taskId, String stageId, Instant startTime) {
        taskStartTimes.putIfAbsent(taskId, startTime);
        taskStageMapping.put(taskId, stageId);
    }

    /** Freezes the peer duration at the first accepted FINISHED transition. */
    public void recordTaskFinish(String taskId) {
        taskFinishTimes.putIfAbsent(taskId, Instant.now());
    }

    /**
     * Produces duration snapshots for tasks in a given stage.
     *
     * @param tasks              the task state machines for the query
     * @param stageId            the stage to snapshot
     * @param speculativeTaskIds task IDs that already have speculative copies
     * @param now                reference instant for duration calculation
     * @return list of task duration records for the specified stage
     */
    public List<TaskDuration> getTaskDurationSnapshots(
            ConcurrentHashMap<String, TaskStateMachine> tasks,
            String stageId, Set<String> speculativeTaskIds, Instant now) {
        if (tasks == null) {
            return List.of();
        }
        List<TaskDuration> snapshots = new ArrayList<>();
        for (Map.Entry<String, TaskStateMachine> entry : tasks.entrySet()) {
            String taskId = entry.getKey();
            if (stageId.equals(taskStageMapping.get(taskId))) {
                snapshots.add(buildTaskDuration(taskId, entry.getValue(),
                        speculativeTaskIds, now));
            }
        }
        return snapshots;
    }

    private TaskDuration buildTaskDuration(String taskId, TaskStateMachine sm,
                                           Set<String> speculativeTaskIds, Instant now) {
        Instant startTime = taskStartTimes.get(taskId);
        boolean completed = sm.getState() == ScheduledTaskState.FINISHED;
        Instant endTime = completed ? taskFinishTimes.getOrDefault(taskId, now) : now;
        long durationMs = startTime != null
                ? Duration.between(startTime, endTime).toMillis()
                : 0L;
        return new TaskDuration(taskId, durationMs, completed,
                speculativeTaskIds.contains(taskId));
    }

    /**
     * Returns the distinct stage IDs for tasks belonging to a given query that are still running.
     *
     * @param tasks the task state machines for the query
     * @return set of stage IDs with at least one running task
     */
    public Set<String> getRunningStageIds(ConcurrentHashMap<String, TaskStateMachine> tasks) {
        if (tasks == null) {
            return Set.of();
        }
        Set<String> stageIds = new java.util.HashSet<>();
        for (Map.Entry<String, TaskStateMachine> entry : tasks.entrySet()) {
            if (entry.getValue().getState() == ScheduledTaskState.RUNNING) {
                String stageId = taskStageMapping.get(entry.getKey());
                if (stageId != null) {
                    stageIds.add(stageId);
                }
            }
        }
        return stageIds;
    }

    /** Removes all timing data for a task (cleanup on completion/cancellation). */
    public void clearTaskTracking(String taskId) {
        taskStartTimes.remove(taskId);
        taskFinishTimes.remove(taskId);
        taskStageMapping.remove(taskId);
        taskDeadlines.remove(taskId);
    }

    /**
     * Returns the recorded start time for a task, or null if not recorded.
     * Used by the parent tracker for latest-started-task resolution.
     */
    public Instant getStartTime(String taskId) {
        return taskStartTimes.get(taskId);
    }
}
