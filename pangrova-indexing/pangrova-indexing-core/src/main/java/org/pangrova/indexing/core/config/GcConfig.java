package org.pangrova.indexing.core.config;

/**
 * Configuration for index garbage collection.
 *
 * @param gcIntervalMinutes     interval between GC runs in minutes (default 60)
 * @param gcGracePeriodMinutes  grace period before purging DROPPED indexes in minutes (default 1440)
 */
public record GcConfig(int gcIntervalMinutes, int gcGracePeriodMinutes) {

    public static final GcConfig DEFAULT = new GcConfig(60, 1440);

    public GcConfig {
        if (gcIntervalMinutes < 1) {
            throw new IllegalArgumentException("gcIntervalMinutes must be >= 1, got " + gcIntervalMinutes);
        }
        if (gcGracePeriodMinutes < 0) {
            throw new IllegalArgumentException(
                    "gcGracePeriodMinutes must be >= 0, got " + gcGracePeriodMinutes);
        }
    }
}
