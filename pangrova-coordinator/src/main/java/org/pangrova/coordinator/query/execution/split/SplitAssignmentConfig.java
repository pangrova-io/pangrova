package org.pangrova.coordinator.query.execution.split;

/**
 * Configuration for split assignment behavior including lease duration,
 * cache affinity assignment, work stealing, and pull-mode enablement.
 *
 * @param leaseDurationMs      maximum time a worker holds a split lease before reclamation
 * @param cacheAffinityAssigner optional assigner for cache-aware split assignment (nullable)
 * @param stealDelayMs         delay before allowing work stealing from affine workers
 * @param pullModeEnabled      whether pull-mode split assignment is active
 */
public record SplitAssignmentConfig(
        long leaseDurationMs,
        CacheAffinityAssigner cacheAffinityAssigner,
        long stealDelayMs,
        boolean pullModeEnabled) {

    private static final long DEFAULT_LEASE_DURATION_MS = 60_000;

    /** Default configuration: push mode, no cache affinity, no steal delay. */
    public static SplitAssignmentConfig defaults() {
        return new SplitAssignmentConfig(
                DEFAULT_LEASE_DURATION_MS, null, 0,
                SplitAssignmentManager.PULL_MODE_DEFAULT);
    }
}
