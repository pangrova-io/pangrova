package org.pangrova.coordinator.resilience.retry;

import org.pangrova.common.api.annotation.ThreadSafe;
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
 * <p>Retry hierarchy (F-RES-04): when a {@link StageRetryManager} is configured,
 * split-level retries are suppressed once the parent stage has decided to retry
 * at stage level. This prevents redundant split retries when a full stage re-execution
 * is imminent.</p>
 */
@ThreadSafe
public class SplitRetryManager {

    private static final Logger LOG = LoggerFactory.getLogger(SplitRetryManager.class);

    public enum SplitState { PENDING, RUNNING, DONE, FAILED, RETRY }

    /**
     * Tracks retry state for a single split. All compound mutations must
     * synchronize on this instance to prevent check-then-act races.
     */
    @ThreadSafe
    static class SplitStatus {
        private SplitState state;
        private int retryCount;
        private PangrovaErrorCode lastError;

        SplitStatus(SplitState state) {
            this.state = state;
            this.retryCount = 0;
        }

        synchronized SplitState getState() { return state; }
        synchronized int getRetryCount() { return retryCount; }
        synchronized PangrovaErrorCode getLastError() { return lastError; }
    }

    private final ConcurrentHashMap<String, SplitStatus> splits = new ConcurrentHashMap<>();
    private final ErrorClassifier classifier;
    private final RetryPolicy retryPolicy;
    private final StageRetryManager stageRetryManager;

    public SplitRetryManager() {
        this(new ErrorClassifier());
    }

    public SplitRetryManager(ErrorClassifier classifier) {
        this(classifier, new RetryPolicy());
    }

    public SplitRetryManager(ErrorClassifier classifier, RetryPolicy retryPolicy) {
        this(classifier, retryPolicy, null);
    }

    /**
     * Creates a SplitRetryManager with stage-level retry awareness.
     * When stageRetryManager is provided, split retries are suppressed if
     * the owning stage has exceeded its threshold and will be retried at stage level.
     */
    public SplitRetryManager(ErrorClassifier classifier, RetryPolicy retryPolicy,
                             StageRetryManager stageRetryManager) {
        this.classifier = classifier;
        this.retryPolicy = retryPolicy;
        this.stageRetryManager = stageRetryManager;
    }

    /**
     * Handles a split failure by classifying the error and deciding whether to retry.
     *
     * <p>If a StageRetryManager is configured and the stage-level retry has been triggered
     * (stage failure ratio exceeded), split-level retry is suppressed to avoid redundant
     * retries before the stage re-executes all tasks.</p>
     *
     * @return true if the split should be retried, false if it should fail permanently
     */
    public boolean handleSplitFailure(String taskId, String splitId, PangrovaErrorCode error) {
        // F-RES-04: suppress split retry if stage-level retry is taking over
        if (stageRetryManager != null) {
            String stageId = extractStageId(taskId);
            if (stageRetryManager.getRetryCount(stageId) > 0) {
                LOG.debug("Split retry suppressed - stage {} is retrying at stage level", stageId);
                return false;
            }
        }
        String key = compositeKey(taskId, splitId);
        SplitStatus status = splits.computeIfAbsent(key, k -> new SplitStatus(SplitState.PENDING));
        ErrorClassifier.ErrorCategory category = classifier.classify(error);
        synchronized (status) {
            if (!retryPolicy.shouldRetry(category, status.retryCount)) {
                status.state = SplitState.FAILED;
                status.lastError = error;
                return false;
            }
            status.retryCount++;
            status.lastError = error;
            status.state = SplitState.RETRY;
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
        splits.put(compositeKey(taskId, splitId), new SplitStatus(SplitState.PENDING));
    }

    /** Marks the split as currently running. */
    public void markRunning(String taskId, String splitId) {
        SplitStatus status = getOrCreate(taskId, splitId);
        synchronized (status) {
            status.state = SplitState.RUNNING;
        }
    }

    /** Marks the split as successfully completed. */
    public void markDone(String taskId, String splitId) {
        SplitStatus status = getOrCreate(taskId, splitId);
        synchronized (status) {
            status.state = SplitState.DONE;
        }
    }

    /** Returns the current state of the split, or null if unknown. */
    public SplitState getState(String taskId, String splitId) {
        SplitStatus status = splits.get(compositeKey(taskId, splitId));
        if (status == null) {
            return null;
        }
        synchronized (status) {
            return status.state;
        }
    }

    private SplitStatus getOrCreate(String taskId, String splitId) {
        return splits.computeIfAbsent(compositeKey(taskId, splitId), k -> new SplitStatus(SplitState.PENDING));
    }

    private String compositeKey(String taskId, String splitId) {
        return taskId + ":" + splitId;
    }

    /**
     * Extracts the stage ID from a task ID. Convention: taskId format is "stageId.taskIndex".
     * Falls back to taskId itself if no separator is found.
     */
    private static String extractStageId(String taskId) {
        int dot = taskId.lastIndexOf('.');
        return dot > 0 ? taskId.substring(0, dot) : taskId;
    }
}
