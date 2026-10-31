package org.pangrova.coordinator.resilience.retry;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.resilience.policy.ErrorClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages retry decisions at the stage level based on task failure ratio.
 *
 * <p>Concurrency strategy: each {@link StageStatus} is guarded by its own
 * intrinsic lock to ensure compound operations on retryCount/totalTasks/failedTasks
 * are atomic. The outer {@code stages} map uses {@link ConcurrentHashMap}.</p>
 */
@ThreadSafe
public class StageRetryManager {

    private static final Logger LOG = LoggerFactory.getLogger(StageRetryManager.class);
    private static final double FAILURE_RATIO_THRESHOLD = 0.3;
    private static final int MAX_STAGE_RETRIES = 2;

    /**
     * Tracks retry state for a single stage. All compound mutations must
     * synchronize on this instance to prevent check-then-act races.
     * The {@code generation} field increments on each retry, allowing
     * callers to discard stale callbacks from previous attempts.
     */
    @ThreadSafe
    static class StageStatus {
        private int retryCount;
        private int totalTasks;
        private int generation;
        private final Set<String> failedTasks = new HashSet<>();

        synchronized int getRetryCount() { return retryCount; }
        synchronized int getTotalTasks() { return totalTasks; }
        synchronized int getFailedTaskCount() { return failedTasks.size(); }
        synchronized int getGeneration() { return generation; }
    }

    private final ConcurrentHashMap<String, StageStatus> stages = new ConcurrentHashMap<>();
    private final ErrorClassifier classifier;

    public StageRetryManager() {
        this(new ErrorClassifier());
    }

    public StageRetryManager(ErrorClassifier classifier) {
        this.classifier = classifier;
    }

    /** Registers a stage with its total task count. */
    public void registerStage(String stageId, int totalTasks) {
        StageStatus status = stages.computeIfAbsent(stageId, k -> new StageStatus());
        synchronized (status) {
            status.totalTasks = totalTasks;
        }
    }

    /** Records a task failure and returns whether a stage-level retry is needed. */
    public boolean onTaskFailure(String stageId, String taskId, PangrovaErrorCode error) {
        StageStatus status = stages.computeIfAbsent(stageId, k -> new StageStatus());
        ErrorClassifier.ErrorCategory category = classifier.classify(error);
        if (!classifier.isRetryable(category)) {
            return false;
        }
        synchronized (status) {
            status.failedTasks.add(taskId);
            return isStageRetryNeeded(status);
        }
    }

    /** Starts a new retry generation for the stage, clearing recorded failures. */
    public void initiateStageRetry(String stageId) {
        StageStatus status = stages.get(stageId);
        if (status == null) {
            return;
        }
        synchronized (status) {
            status.retryCount++;
            status.generation++;
            status.failedTasks.clear();
            LOG.info("Stage retry initiated, stageId={}, retryCount={}, generation={}",
                    stageId, status.retryCount, status.generation);
        }
    }

    /** Returns how many retry generations have been initiated for the stage. */
    public int getRetryCount(String stageId) {
        StageStatus status = stages.get(stageId);
        if (status == null) {
            return 0;
        }
        synchronized (status) {
            return status.retryCount;
        }
    }

    /**
     * Returns the current generation for the given stage.
     * Callers should capture this value when dispatching tasks and
     * compare it on callback to discard stale responses.
     */
    public int getGeneration(String stageId) {
        StageStatus status = stages.get(stageId);
        if (status == null) {
            return 0;
        }
        synchronized (status) {
            return status.generation;
        }
    }

    /**
     * Returns true if the given generation matches the current generation
     * for the stage. Used to discard callbacks from stale retry attempts.
     */
    public boolean isCurrentGeneration(String stageId, int generation) {
        StageStatus status = stages.get(stageId);
        if (status == null) {
            return generation == 0;
        }
        synchronized (status) {
            return status.generation == generation;
        }
    }

    /**
     * Reads the compound retry decision while holding the same StageStatus mutation lock.
     */
    private boolean isStageRetryNeeded(StageStatus status) {
        synchronized (status) {
            if (status.retryCount >= MAX_STAGE_RETRIES || status.totalTasks == 0) {
                return false;
            }
            double failureRatio = (double) status.failedTasks.size() / status.totalTasks;
            return failureRatio >= FAILURE_RATIO_THRESHOLD;
        }
    }
}
