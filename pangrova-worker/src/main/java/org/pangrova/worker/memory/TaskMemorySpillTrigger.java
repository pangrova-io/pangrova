package org.pangrova.worker.memory;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Triggers spill-to-disk for hash tables and sort buffers when a task
 * exceeds its memory reservation, preventing OOM failures.
 *
 * <p>When memory usage exceeds the configured threshold (as a fraction of
 * the task's reservation), this trigger signals the native engine to spill
 * in-memory data structures to disk. Only if spill fails or disk is full
 * does the task fail with OOM.</p>
 *
 * <p>Concurrency strategy: AtomicLong for counters. The trigger is called
 * from the memory allocation hot path and must be lock-free.</p>
 */
@ThreadSafe
public final class TaskMemorySpillTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemorySpillTrigger.class);

    /** Fraction of the threshold retained as headroom after a spill. */
    private static final double SPILL_TARGET_RATIO = 0.8;

    private final double spillThresholdRatio;
    private final AtomicLong spillCount = new AtomicLong(0);
    private final AtomicLong spillBytes = new AtomicLong(0);
    private final AtomicLong failedSpillCount = new AtomicLong(0);

    /**
     * Creates a spill trigger with the given threshold ratio.
     *
     * @param spillThresholdRatio fraction of reservation at which spill is triggered (0.0–1.0)
     */
    public TaskMemorySpillTrigger(double spillThresholdRatio) {
        if (spillThresholdRatio <= 0.0 || spillThresholdRatio > 1.0) {
            throw new IllegalArgumentException(
                    "spillThresholdRatio must be in (0.0, 1.0]: " + spillThresholdRatio);
        }
        this.spillThresholdRatio = spillThresholdRatio;
    }

    /**
     * Evaluates whether a spill should be triggered for the given task.
     *
     * @param taskId          the task identifier
     * @param currentUsage    current memory usage in bytes
     * @param reservationBytes total memory reservation for the task
     * @return the spill decision
     */
    public SpillDecision evaluate(String taskId, long currentUsage, long reservationBytes) {
        if (reservationBytes <= 0) {
            return SpillDecision.NO_SPILL;
        }
        double usageRatio = (double) currentUsage / reservationBytes;
        if (usageRatio < spillThresholdRatio) {
            return SpillDecision.NO_SPILL;
        }
        long bytesToSpill = currentUsage - (long) (reservationBytes * spillThresholdRatio * SPILL_TARGET_RATIO);
        if (bytesToSpill <= 0) {
            return SpillDecision.NO_SPILL;
        }
        LOG.info("event=spill_triggered taskId={} usageRatio={} threshold={} bytesToSpill={}",
                taskId, String.format("%.2f", usageRatio), spillThresholdRatio, bytesToSpill);
        return new SpillDecision(true, bytesToSpill);
    }

    /**
     * Records a successful spill operation.
     */
    public void recordSpill(long bytesSpilled) {
        spillCount.incrementAndGet();
        spillBytes.addAndGet(bytesSpilled);
    }

    /**
     * Records a failed spill attempt.
     */
    public void recordSpillFailure() {
        failedSpillCount.incrementAndGet();
    }

    /**
     * Returns the total number of spill operations triggered.
     */
    public long spillCount() {
        return spillCount.get();
    }

    /**
     * Returns the total bytes spilled to disk.
     */
    public long spillBytes() {
        return spillBytes.get();
    }

    /**
     * Returns the number of failed spill attempts.
     */
    public long failedSpillCount() {
        return failedSpillCount.get();
    }

    /**
     * Returns the configured spill threshold ratio.
     */
    public double spillThresholdRatio() {
        return spillThresholdRatio;
    }

    /**
     * Represents the decision of whether to spill and how much.
     */
    public record SpillDecision(boolean shouldSpill, long bytesToSpill) {
        public static final SpillDecision NO_SPILL = new SpillDecision(false, 0);
    }
}
