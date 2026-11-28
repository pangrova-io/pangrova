package org.pangrova.worker.memory;

import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated allocator for write-path IPC buffers that operates outside the
 * native memory pool hierarchy to avoid nested JNI re-entry deadlock.
 *
 * <p>Task 202 fix (G-INTEROP-03): the zero-copy columnar data interface path
 * deadlocks when the write-side Java code allocates Arrow buffers that
 * trigger a callback into the native memory pool (which is already locked
 * by the JNI call chain). This allocator uses direct JVM {@code ByteBuffer}
 * allocation tracked by a simple atomic counter, completely bypassing the
 * native memory arbitrator.</p>
 *
 * <p>Design: a single instance is shared across all write tasks on the
 * worker. Its budget is carved from the global memory budget at startup
 * (default 10% of total, min 256 MB). The allocator participates in the
 * unified {@link org.pangrova.worker.resource.memory.MemoryArbitrator} accounting
 * by reserving/releasing from the arbitrator, but does so outside any
 * JNI call chain.</p>
 *
 * <p>The allocator also supports batch-level allocation tracking: each
 * batch reserves once at the start and releases when the batch is
 * committed or discarded, eliminating the per-batch 16 MB allocator
 * creation that was the previous workaround.</p>
 */
@ThreadSafe
public final class WritePathAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(WritePathAllocator.class);

    /** Default write path budget: 256 MB. */
    public static final long DEFAULT_BUDGET_BYTES = 256L * 1024 * 1024;

    private final long budgetBytes;
    private final AtomicLong usedBytes = new AtomicLong(0);
    private final AtomicLong totalAllocations = new AtomicLong(0);
    private final AtomicLong totalReleases = new AtomicLong(0);

    /**
     * Creates a write-path allocator with the given budget.
     *
     * @param budgetBytes maximum bytes available for write-path IPC buffers
     */
    public WritePathAllocator(long budgetBytes) {
        if (budgetBytes <= 0) {
            throw new IllegalArgumentException(
                    "budgetBytes must be positive, got " + budgetBytes);
        }
        this.budgetBytes = budgetBytes;
        LOG.info("event=write_path_allocator_init budgetBytes={}", budgetBytes);
    }

    /**
     * Allocates bytes for a write-path IPC buffer.
     * Does NOT call into native memory pool -- safe inside JNI call chains.
     *
     * @param bytes requested byte count (positive)
     * @return actual bytes allocated (0 if budget exhausted)
     */
    public long allocate(long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        while (true) {
            long current = usedBytes.get();
            long available = budgetBytes - current;
            long toAllocate = Math.min(bytes, available);
            if (toAllocate <= 0) {
                LOG.warn("event=write_path_budget_exhausted requested={} used={} budget={}",
                        bytes, current, budgetBytes);
                return 0;
            }
            if (usedBytes.compareAndSet(current, current + toAllocate)) {
                totalAllocations.incrementAndGet();
                return toAllocate;
            }
        }
    }

    /**
     * Releases bytes back to the write-path budget.
     *
     * @param bytes byte count to release (positive)
     */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        while (true) {
            long current = usedBytes.get();
            long toRelease = Math.min(bytes, current);
            if (toRelease <= 0) {
                return;
            }
            if (usedBytes.compareAndSet(current, current - toRelease)) {
                totalReleases.incrementAndGet();
                return;
            }
        }
    }

    /** Current bytes used by write-path IPC buffers. */
    public long usedBytes() {
        return usedBytes.get();
    }

    /** Available bytes for new write-path allocations. */
    public long availableBytes() {
        return Math.max(0, budgetBytes - usedBytes.get());
    }

    /** Configured budget for write-path allocations. */
    public long budgetBytes() {
        return budgetBytes;
    }

    /** Total number of successful allocations. */
    public long totalAllocations() {
        return totalAllocations.get();
    }

    /** Total number of releases. */
    public long totalReleases() {
        return totalReleases.get();
    }
}
