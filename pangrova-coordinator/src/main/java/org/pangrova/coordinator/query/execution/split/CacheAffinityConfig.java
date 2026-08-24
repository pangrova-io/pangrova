package org.pangrova.coordinator.query.execution.split;

/**
 * Configuration for NVMe cache affinity scheduling.
 *
 * @param enabled       whether cache-affine split assignment is active
 * @param stealDelayMs  how long a split waits for its affine Worker before becoming
 *                      available to any Worker (0 = no delay)
 */
public record CacheAffinityConfig(
        boolean enabled,
        long stealDelayMs
) {

    public static final CacheAffinityConfig DISABLED =
            new CacheAffinityConfig(false, 0);

    public static final CacheAffinityConfig DEFAULT =
            new CacheAffinityConfig(true, 500);

    public CacheAffinityConfig {
        if (stealDelayMs < 0) {
            throw new IllegalArgumentException(
                    "stealDelayMs must be non-negative, got: " + stealDelayMs);
        }
    }
}
