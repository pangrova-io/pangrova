package org.pangrova.metadata.service;

/**
 * Configuration for periodic metadata cleanup tasks.
 *
 * <p>Loaded from YAML under {@code metadata.cleanup}:
 * <pre>
 * metadata:
 *   cleanup:
 *     intervalMinutes: 10
 *     invalidationCleanupEnabled: true
 *     cacheCleanupEnabled: true
 * </pre>
 *
 * @param intervalMinutes              interval between cleanup runs (default 10)
 * @param invalidationCleanupEnabled   whether to purge old invalidation log entries
 * @param cacheCleanupEnabled          whether to evict stale L1 cache entries
 */
public record MetadataCleanupConfig(
        long intervalMinutes,
        boolean invalidationCleanupEnabled,
        boolean cacheCleanupEnabled
) {
    /** Default configuration: all cleanup tasks enabled, 10-minute interval. */
    public static final MetadataCleanupConfig DEFAULT =
            new MetadataCleanupConfig(10, true, true);

    public MetadataCleanupConfig {
        if (intervalMinutes <= 0) {
            throw new IllegalArgumentException(
                    "intervalMinutes must be positive, got " + intervalMinutes);
        }
    }
}
