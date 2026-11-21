package org.pangrova.indexing.config;

/**
 * Configuration for index consistency checking and auto-rebuild.
 *
 * @param checkIntervalMinutes          interval between consistency checks in minutes (default 30)
 * @param rebuildThreshold              coverage ratio below which auto-rebuild triggers (default 0.8)
 * @param autoRebuildEnabled            whether auto-rebuild is enabled (default true)
 * @param buildStuckDetectionMultiplier multiplier for detecting stuck builds (default 2)
 */
public record ConsistencyConfig(
        int checkIntervalMinutes,
        double rebuildThreshold,
        boolean autoRebuildEnabled,
        int buildStuckDetectionMultiplier
) {

    public static final ConsistencyConfig DEFAULT = new ConsistencyConfig(30, 0.8, true, 2);

    public ConsistencyConfig {
        if (checkIntervalMinutes < 1) {
            throw new IllegalArgumentException(
                    "checkIntervalMinutes must be >= 1, got " + checkIntervalMinutes);
        }
        if (rebuildThreshold < 0.0 || rebuildThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "rebuildThreshold must be in [0.0, 1.0], got " + rebuildThreshold);
        }
        if (buildStuckDetectionMultiplier < 1) {
            throw new IllegalArgumentException(
                    "buildStuckDetectionMultiplier must be >= 1, got " + buildStuckDetectionMultiplier);
        }
    }
}
