package org.pangrova.streaming.core.maintenance;

import java.time.Duration;

/**
 * Configuration for maintenance scheduling.
 */
public record MaintenanceConfig(
    Duration compactionInterval,
    Duration orphanCleanupInterval,
    Duration snapshotExpirationInterval,
    Duration compactionScanInterval,
    Duration orphanCleanupScanInterval,
    int minFilesToCompact,
    int maxEqualityDeleteFiles,
    Duration orphanFileAge,
    Duration snapshotRetention,
    int snapshotRetainCount,
    int threads,
    int maxConcurrentTasks,
    int taskQueueCapacity,
    Duration taskTimeout
) {
    public static MaintenanceConfig defaults() {
        return new MaintenanceConfig(
            Duration.ofMinutes(60), Duration.ofHours(6),
            Duration.ofHours(12), Duration.ofMinutes(60),
            Duration.ofHours(24), 5, 10,
            Duration.ofDays(3), Duration.ofDays(7), 100,
            4, 4, 16, Duration.ofMinutes(30)
        );
    }
}
