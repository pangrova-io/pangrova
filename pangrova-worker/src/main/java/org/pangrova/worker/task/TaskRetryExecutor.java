package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages task retry logic for transient failures with exponential backoff.
 *
 * <p>When a task fails with a transient error (network timeout, OOM after spill),
 * this executor determines whether the task should be retried or reported as
 * permanently failed. Retry attempts use exponential backoff to avoid
 * overwhelming recovering services.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for retry counters, AtomicInteger
 * per task for thread-safe increment. No locks held during backoff sleep.</p>
 */
@ThreadSafe
public final class TaskRetryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRetryExecutor.class);

    private final TaskRetryConfig config;
    private final ConcurrentHashMap<String, AtomicInteger> retryCounters = new ConcurrentHashMap<>();

    /** Creates an executor with the given retry configuration. */
    public TaskRetryExecutor(TaskRetryConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
    }

    /**
     * Determines whether a failed task should be retried.
     *
     * @param taskId  the task identifier
     * @param failure the exception that caused the failure
     * @return true if the task should be retried, false if it should be reported as permanent failure
     */
    public boolean shouldRetry(String taskId, Exception failure) {
        if (!TransientFailureClassifier.isTransient(failure)) {
            LOG.debug("event=task_retry_skip_permanent taskId={} error={}", taskId, failure.getMessage());
            return false;
        }
        AtomicInteger counter = retryCounters.computeIfAbsent(taskId, k -> new AtomicInteger(0));
        int attempt = counter.get();
        if (attempt >= config.maxRetries()) {
            LOG.warn("event=task_retry_exhausted taskId={} attempts={} maxRetries={} error={}",
                    taskId, attempt, config.maxRetries(), failure.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Records a retry attempt and computes the backoff delay.
     *
     * @param taskId the task identifier
     * @return the backoff delay in milliseconds before the retry should be attempted
     */
    public long recordRetryAndGetBackoff(String taskId) {
        AtomicInteger counter = retryCounters.computeIfAbsent(taskId, k -> new AtomicInteger(0));
        int attempt = counter.getAndIncrement();
        long backoffMs = config.computeBackoffMs(attempt);
        LOG.info("event=task_retry_scheduled taskId={} attempt={} backoffMs={}", taskId, attempt + 1, backoffMs);
        return backoffMs;
    }

    /**
     * Returns the current retry count for a task.
     */
    public int getRetryCount(String taskId) {
        AtomicInteger counter = retryCounters.get(taskId);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Clears retry state for a completed or permanently failed task.
     * Should be called when a task reaches a terminal state.
     */
    public void clearRetryState(String taskId) {
        retryCounters.remove(taskId);
    }

    /**
     * Returns the retry configuration.
     */
    public TaskRetryConfig config() {
        return config;
    }
}
