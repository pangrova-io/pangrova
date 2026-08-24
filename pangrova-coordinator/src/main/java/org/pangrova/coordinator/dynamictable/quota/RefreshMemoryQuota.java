package org.pangrova.coordinator.dynamictable.quota;

/**
 * Memory quota enforcement for Dynamic Table refresh operations.
 *
 * <p>Each refresh allocates memory via {@link #allocate(long)} before
 * processing and releases via {@link #release(long)} after completion
 * (success or failure). Exceeding the quota throws
 * {@link RefreshMemoryQuotaExceededException}.</p>
 */
public interface RefreshMemoryQuota {

    /**
     * Allocate the requested bytes from the refresh memory pool.
     *
     * @param bytes number of bytes to allocate
     * @throws RefreshMemoryQuotaExceededException if quota is exceeded
     */
    void allocate(long bytes);

    /**
     * Release previously allocated bytes back to the pool.
     *
     * @param bytes number of bytes to release
     */
    void release(long bytes);

    /** Return the currently used bytes. */
    long usedBytes();

    /** Return the total quota in bytes. */
    long totalQuotaBytes();

    /** No-op implementation that never rejects allocations. */
    static RefreshMemoryQuota unlimited() {
        return new UnlimitedRefreshMemoryQuota();
    }
}
