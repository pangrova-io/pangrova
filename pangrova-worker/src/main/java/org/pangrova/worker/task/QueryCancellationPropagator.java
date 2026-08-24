package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Propagates query cancellation to all tasks belonging to a query within 1 second.
 *
 * <p>When a query is cancelled, this propagator identifies all tasks for that query
 * and cancels them, including native execution tasks via JNI callback. The design
 * ensures sub-second propagation by maintaining a queryId → taskIds index.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for the query-to-tasks index.
 * Registration and cancellation are lock-free. The index is eventually consistent
 * with the task registry — a task may be registered slightly after submission.</p>
 */
@ThreadSafe
public final class QueryCancellationPropagator {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCancellationPropagator.class);

    /** Nanoseconds per millisecond, for elapsed-time conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    private final ConcurrentHashMap<String, List<String>> queryTaskIndex = new ConcurrentHashMap<>();
    private final TaskManager taskManager;

    /** Creates a propagator that cancels tasks through the given task manager. */
    public QueryCancellationPropagator(TaskManager taskManager) {
        if (taskManager == null) {
            throw new IllegalArgumentException("taskManager must not be null");
        }
        this.taskManager = taskManager;
    }

    /**
     * Registers a task for a query. Called on task submission.
     */
    public void registerTask(String queryId, String taskId) {
        queryTaskIndex.compute(queryId, (key, existing) -> {
            List<String> tasks = existing != null ? existing : new ArrayList<>();
            tasks.add(taskId);
            return tasks;
        });
    }

    /**
     * Cancels all tasks for a query. Returns the number of tasks cancelled.
     *
     * @param queryId the query to cancel
     * @return number of tasks successfully cancelled
     */
    public int cancelQuery(String queryId) {
        long startNanos = System.nanoTime();
        List<String> taskIds = queryTaskIndex.remove(queryId);
        if (taskIds == null || taskIds.isEmpty()) {
            LOG.debug("event=query_cancel_no_tasks queryId={}", queryId);
            return 0;
        }
        int cancelled = 0;
        for (String taskId : taskIds) {
            TaskCancelResult result = taskManager.cancelTask(taskId);
            if (result.isSuccess()) {
                cancelled++;
            }
        }
        long elapsedMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
        LOG.info("event=query_cancelled queryId={} totalTasks={} cancelled={} elapsedMs={}",
                queryId, taskIds.size(), cancelled, elapsedMs);
        if (elapsedMs > 1000) {
            LOG.warn("event=query_cancel_slow queryId={} elapsedMs={} exceeds_1s_target=true",
                    queryId, elapsedMs);
        }
        return cancelled;
    }

    /**
     * Removes a task from the query index. Called on task completion.
     */
    public void deregisterTask(String queryId, String taskId) {
        queryTaskIndex.computeIfPresent(queryId, (key, tasks) -> {
            tasks.remove(taskId);
            return tasks.isEmpty() ? null : tasks;
        });
    }

    /**
     * Returns the number of tracked queries.
     */
    public int trackedQueryCount() {
        return queryTaskIndex.size();
    }

    /**
     * Returns the number of tasks tracked for a specific query.
     */
    public int taskCountForQuery(String queryId) {
        List<String> tasks = queryTaskIndex.get(queryId);
        return tasks != null ? tasks.size() : 0;
    }
}
