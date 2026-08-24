package org.pangrova.worker.execution;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.pangrova.spi.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls admission of native tasks to prevent fan-out deadlock.
 *
 * <p>Task 200 fix (G-EXEC-04): when the number of concurrently executing
 * native tasks exceeds the native driver pool size, all pool threads
 * can be occupied by blocked orchestration waits, starving actual native
 * driver execution. This controller limits concurrent native tasks to
 * strictly less than the pool size, guaranteeing at least one free thread
 * for actual driver work.</p>
 *
 * <p>Model: Spark's TaskSchedulerImpl limits concurrent tasks to available
 * cores minus headroom. Gluten uses a separate submission queue.</p>
 */
@ThreadSafe
public final class NativeTaskAdmissionController {

    private static final Logger LOG = LoggerFactory.getLogger(NativeTaskAdmissionController.class);

    private final Semaphore permits;
    private final int maxConcurrentTasks;

    /**
     * Creates an admission controller.
     *
     * @param maxConcurrentTasks maximum concurrent native tasks; must be at least 1
     */
    public NativeTaskAdmissionController(int maxConcurrentTasks) {
        if (maxConcurrentTasks < 1) {
            throw new IllegalArgumentException(
                    "maxConcurrentTasks must be >= 1, got " + maxConcurrentTasks);
        }
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.permits = new Semaphore(maxConcurrentTasks, true);
        LOG.info("event=admission_controller_created maxConcurrentTasks={}", maxConcurrentTasks);
    }

    /**
     * Acquires a task slot. Blocks if no slots are available.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if a slot was acquired, false if timed out
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public boolean acquire(long timeoutMs) throws InterruptedException {
        boolean acquired = permits.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        if (!acquired) {
            LOG.warn("event=admission_timeout availableSlots={} maxConcurrentTasks={}",
                    permits.availablePermits(), maxConcurrentTasks);
        }
        return acquired;
    }

    /**
     * Releases a task slot, making it available for other tasks.
     */
    public void release() {
        permits.release();
    }

    /** Returns the number of currently available task slots. */
    public int availableSlots() {
        return permits.availablePermits();
    }

    /** Returns the maximum number of concurrent native tasks. */
    public int maxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    /** Returns the number of tasks currently executing. */
    public int activeTaskCount() {
        return maxConcurrentTasks - permits.availablePermits();
    }
}
