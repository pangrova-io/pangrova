package org.pangrova.resource.tenant;

/**
 * Per-tenant resource quota configuration.
 *
 * <p>Immutable value object defining the maximum resources a single tenant
 * may consume. Used by {@link TenantResourceTracker} to enforce isolation.</p>
 *
 * @param maxMemoryBytes         maximum memory budget in bytes (≤0 means unlimited)
 * @param maxConcurrentQueries   maximum concurrent queries (≤0 means unlimited)
 * @param maxCpuTimePerQueryMs   maximum CPU time per query in milliseconds (≤0 means unlimited)
 * @param maxScanBytesPerQuery   maximum scan bytes per query (≤0 means unlimited)
 */
public record TenantResourceQuota(
        long maxMemoryBytes,
        int maxConcurrentQueries,
        long maxCpuTimePerQueryMs,
        long maxScanBytesPerQuery
) {

    /**
     * Convenience constructor for callers that only need memory and concurrency limits.
     * Runtime quota fields default to unlimited.
     */
    public TenantResourceQuota(long maxMemoryBytes, int maxConcurrentQueries) {
        this(maxMemoryBytes, maxConcurrentQueries, -1, -1);
    }

    /** Unlimited quota — no enforcement. */
    public static final TenantResourceQuota UNLIMITED =
            new TenantResourceQuota(-1, -1, -1, -1);

    public boolean hasMemoryLimit() {
        return maxMemoryBytes > 0;
    }

    public boolean hasConcurrencyLimit() {
        return maxConcurrentQueries > 0;
    }

    public boolean hasCpuTimeLimit() {
        return maxCpuTimePerQueryMs > 0;
    }

    public boolean hasScanBytesLimit() {
        return maxScanBytesPerQuery > 0;
    }
}
