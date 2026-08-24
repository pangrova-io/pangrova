package org.pangrova.resource.memory;

/**
 * Thrown when a memory allocation request cannot be fulfilled.
 * Carries full context about the pool state at the time of failure.
 */
public final class InsufficientMemoryException extends RuntimeException {

    /** Immutable snapshot of pool state at the time of allocation failure. */
    public record PoolSnapshot(String poolName, long availableBytes, long usedBytes, long maxBytes) {}

    private final long requestedBytes;
    private final PoolSnapshot pool;

    public InsufficientMemoryException(long requestedBytes, PoolSnapshot pool) {
        super(formatMessage(requestedBytes, pool));
        this.requestedBytes = requestedBytes;
        this.pool = pool;
    }

    public long getRequestedBytes() {
        return requestedBytes;
    }

    public PoolSnapshot getPool() {
        return pool;
    }

    public String getPoolName() {
        return pool.poolName();
    }

    public long getAvailableBytes() {
        return pool.availableBytes();
    }

    public long getUsedBytes() {
        return pool.usedBytes();
    }

    public long getMaxBytes() {
        return pool.maxBytes();
    }

    private static String formatMessage(long requestedBytes, PoolSnapshot pool) {
        return String.format(
                "Insufficient memory in pool '%s': requested=%d, available=%d, used=%d, max=%d",
                pool.poolName(), requestedBytes, pool.availableBytes(),
                pool.usedBytes(), pool.maxBytes());
    }
}
