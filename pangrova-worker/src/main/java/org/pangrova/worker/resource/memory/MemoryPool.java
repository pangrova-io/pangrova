package org.pangrova.worker.resource.memory;

import org.pangrova.worker.exception.DoubleReleaseException;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract base for hierarchical memory pools.
 * <p>
 * Each pool tracks a reservation against a maximum byte budget.
 * Child pools reserve from their parent, ensuring the invariant:
 * sum(child.usedBytes) &lt;= parent.maxBytes.
 * <p>
 * Concurrency strategy: all mutable state is managed via {@link AtomicLong}
 * with CAS loops, so no external synchronization is required.
 */
@ThreadSafe
public abstract sealed class MemoryPool
        permits RootMemoryPool, NodeMemoryPool, QueryMemoryPool, TaskMemoryPool,
                OperatorMemoryPool, ArbitratedMemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryPool.class);

    private final String name;
    private final long maxBytes;
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicLong doubleReleaseCount = new AtomicLong();

    protected MemoryPool(String name, long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be non-negative, got " + maxBytes);
        }
        this.name = name;
        this.maxBytes = maxBytes;
    }

    /**
     * Attempts to reserve the requested number of bytes.
     *
     * @param bytes number of bytes to reserve (must be positive)
     * @return the actual number of bytes reserved (may be less than requested)
     */
    public long tryReserve(long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        while (true) {
            long current = usedBytes.get();
            long available = maxBytes - current;
            long toReserve = Math.min(bytes, available);
            if (toReserve <= 0) {
                return 0;
            }
            if (usedBytes.compareAndSet(current, current + toReserve)) {
                return toReserve;
            }
        }
    }

    /**
     * Releases previously reserved bytes back to this pool.
     * <p>
     * Asserts that the current usage is at least {@code bytes}. If the caller
     * attempts to free more than currently reserved (indicating a double-release
     * or accounting bug), this method throws immediately and logs an ERROR.
     *
     * @param bytes number of bytes to free (must be positive)
     * @throws DoubleReleaseException if bytes exceeds current usage
     */
    public void free(long bytes) {
        if (bytes <= 0) {
            return;
        }
        while (true) {
            long current = usedBytes.get();
            if (current < bytes) {
                doubleReleaseCount.incrementAndGet();
                LOG.error("event=double_release_detected pool={} releaseBytes={} currentUsed={} maxBytes={}",
                        name, bytes, current, maxBytes);
                throw new DoubleReleaseException(name, bytes, current);
            }
            if (usedBytes.compareAndSet(current, current - bytes)) {
                return;
            }
        }
    }

    /**
     * Returns the number of double-release violations detected on this pool.
     * Intended for metrics and alerting.
     */
    public long getDoubleReleaseCount() {
        return doubleReleaseCount.get();
    }

    public String getName() {
        return name;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    public long getFreeBytes() {
        return maxBytes - usedBytes.get();
    }

    /**
     * Returns the utilization ratio of this pool (0.0 to 1.0).
     * Useful for metrics and monitoring.
     *
     * @return usage ratio, or 0.0 if maxBytes is 0
     */
    public double getUtilization() {
        if (maxBytes == 0) {
            return 0.0;
        }
        return (double) usedBytes.get() / maxBytes;
    }

    /**
     * Returns a metrics snapshot of this pool's current state.
     * Intended for observability integration.
     */
    public MemoryPoolMetrics getMetrics() {
        long used = usedBytes.get();
        return new MemoryPoolMetrics(name, maxBytes, used, maxBytes - used);
    }

    /**
     * Immutable snapshot of memory pool metrics for observability.
     *
     * @param poolName  name of the pool
     * @param maxBytes  maximum capacity in bytes
     * @param usedBytes currently used bytes
     * @param freeBytes currently free bytes
     */
    public record MemoryPoolMetrics(String poolName, long maxBytes, long usedBytes, long freeBytes) {}
}
