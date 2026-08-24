package org.pangrova.coordinator.dynamictable.quota;

/**
 * No-op memory quota that never rejects allocations.
 * Used as default when no resource manager is configured.
 */
public final class UnlimitedRefreshMemoryQuota implements RefreshMemoryQuota {

    @Override
    public void allocate(long bytes) {
        // no-op: unlimited quota
    }

    @Override
    public void release(long bytes) {
        // no-op
    }

    @Override
    public long usedBytes() {
        return 0;
    }

    @Override
    public long totalQuotaBytes() {
        return Long.MAX_VALUE;
    }
}
