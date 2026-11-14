package org.pangrova.worker.resource.thread;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks thread budget allocation across query, IO, and scheduler pools.
 *
 * <p>This is a budget tracker (counter), not a thread pool manager — it does not
 * create or manage actual {@link java.util.concurrent.ExecutorService} instances.
 * Callers use it to reserve/release thread slots against a global budget.</p>
 *
 * <p>Concurrency strategy: all mutable state is managed via {@link AtomicInteger}
 * with CAS loops, ensuring thread-safe allocation and release without locks.</p>
 */
@ThreadSafe
public class ThreadBudgetTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadBudgetTracker.class);
    private final int maxThreads;
    private final AtomicInteger allocatedThreads = new AtomicInteger(0);

    public ThreadBudgetTracker(int queryThreads, int ioThreads, int schedulerThreads) {
        this.maxThreads = queryThreads + ioThreads + schedulerThreads;
        LOG.info("ThreadBudgetTracker initialized: query={}, io={}, scheduler={}, max={}",
                queryThreads, ioThreads, schedulerThreads, maxThreads);
    }

    /**
     * Allocates threads using CAS loop to prevent TOCTOU race.
     */
    public int allocateThreads(int requested) {
        if (requested <= 0) {
            return 0;
        }
        while (true) {
            int current = allocatedThreads.get();
            int available = maxThreads - current;
            int toAllocate = Math.min(requested, available);
            if (toAllocate <= 0) {
                return 0;
            }
            if (allocatedThreads.compareAndSet(current, current + toAllocate)) {
                return toAllocate;
            }
        }
    }

    /**
     * Releases threads using CAS loop to ensure allocatedThreads never goes below zero.
     */
    public void releaseThreads(int count) {
        if (count <= 0) {
            return;
        }
        while (true) {
            int current = allocatedThreads.get();
            int toRelease = Math.min(count, current);
            if (toRelease <= 0) {
                return;
            }
            if (allocatedThreads.compareAndSet(current, current - toRelease)) {
                return;
            }
        }
    }

    public int getAvailableThreads() {
        return maxThreads - allocatedThreads.get();
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public int getAllocatedThreads() {
        return allocatedThreads.get();
    }

    /**
     * Shuts down the thread budget tracker, releasing all allocated slots.
     */
    public void shutdown() {
        int released = allocatedThreads.getAndSet(0);
        LOG.info("ThreadBudgetTracker shutdown: released {} threads", released);
    }
}
