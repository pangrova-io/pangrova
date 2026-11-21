package org.pangrova.coordinator.resilience.retry;

import org.pangrova.common.enums.ErrorCategory;
import org.pangrova.coordinator.enums.SplitRetryState;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.resilience.policy.ErrorClassifier;
import org.pangrova.common.resilience.policy.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages retry state for individual task splits.
 * Uses RetryPolicy for retry decisions and backoff computation.
 *
 * <p>Concurrency strategy: each {@link SplitStatus} is guarded by its own
 * intrinsic lock to ensure compound read-write operations on state/retryCount/lastError
 * are atomic. The outer {@code splits} map uses {@link ConcurrentHashMap} for safe
 * concurrent access to status entries.</p>
 *
 */
@ThreadSafe
public class SplitRetryManager {

    private static final Logger LOG = LoggerFactory.getLogger(SplitRetryManager.class);

    /**
     * Tracks retry state for a single split. All compound mutations must
     * synchronize on this instance to prevent check-then-act races.
     */
    @ThreadSafe
    static class SplitStatus {
        private SplitRetryState state;
        private int retryCount;
        private PangrovaErrorCode lastError;

        SplitStatus(SplitRetryState state) {
            this.state = state;
            this.retryCount = 0;
        }

        synchronized SplitRetryState getState() { return state; }
        synchronized int getRetryCount() { return retryCount; }
        synchronized PangrovaErrorCode getLastError() { return lastError; }
    }

    private final ConcurrentHashMap<String, SplitStatus> splits = new ConcurrentHashMap<>();
    private final ErrorClassifier classifier;
    private final RetryPolicy retryPolicy;

    public SplitRetryManager() {
        this(new ErrorClassifier());
    }

    public SplitRetryManager(ErrorClassifier classifier) {
        this(classifier, new RetryPolicy());
    }

    public SplitRetryManager(ErrorClassifier classifier, RetryPolicy retryPolicy) {
        this.classifier = classifier;
        this.retryPolicy = retryPolicy;
    }

    /**
     * Handles a split failure by classifying the error and deciding whether to retry.
     *
     * @return true if the split should be retried, false if it should fail permanently
     */
    public boolean handleSplitFailure(String taskId, String splitId, PangrovaErrorCode error) {
        String key = compositeKey(taskId, splitId);
        SplitStatus status = splits.computeIfAbsent(key, k -> new SplitStatus(SplitRetryState.PENDING));
        ErrorCategory category = classifier.classify(error);
        synchronized (status) {
            if (!retryPolicy.shouldRetry(category, status.retryCount)) {
                status.state = SplitRetryState.FAILED;
                status.lastError = error;
                return false;
            }
            status.retryCount++;
            status.lastError = error;
            status.state = SplitRetryState.RETRY;
            Duration backoff = retryPolicy.getBackoff(status.retryCount - 1);
            LOG.info("Split retry scheduled, retryCount={}, backoffMs={}", status.retryCount, backoff.toMillis());
            return true;
        }
    }

    /**
     * Returns the backoff duration for the current retry attempt of a split.
     */
    public Duration getRetryBackoff(String taskId, String splitId) {
        SplitStatus status = splits.get(compositeKey(taskId, splitId));
        if (status == null) {
            return Duration.ZERO;
        }
        synchronized (status) {
            return retryPolicy.getBackoff(status.retryCount);
        }
    }

    public void markPending(String taskId, String splitId) {
        splits.put(compositeKey(taskId, splitId), new SplitStatus(SplitRetryState.PENDING));
    }

    /** Marks the split as currently running. */
    public void markRunning(String taskId, String splitId) {
        SplitStatus status = getOrCreate(taskId, splitId);
        synchronized (status) {
            status.state = SplitRetryState.RUNNING;
        }
    }

    /** Marks the split as successfully completed. */
    public void markDone(String taskId, String splitId) {
        SplitStatus status = getOrCreate(taskId, splitId);
        synchronized (status) {
            status.state = SplitRetryState.DONE;
        }
    }

    /** Returns the current state of the split, or null if unknown. */
    public SplitRetryState getState(String taskId, String splitId) {
        SplitStatus status = splits.get(compositeKey(taskId, splitId));
        if (status == null) {
            return null;
        }
        synchronized (status) {
            return status.state;
        }
    }

    private SplitStatus getOrCreate(String taskId, String splitId) {
        return splits.computeIfAbsent(compositeKey(taskId, splitId), k -> new SplitStatus(SplitRetryState.PENDING));
    }

    private String compositeKey(String taskId, String splitId) {
        return taskId + ":" + splitId;
    }
}
