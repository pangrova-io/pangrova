package org.pangrova.streaming.maintenance;

import java.time.Duration;

/**
 * Policy defining snapshot expiration rules for a table.
 *
 * @param retentionPeriod how long snapshots are retained before expiration
 * @param retainCount minimum number of snapshots to keep regardless of age
 * @param expirationInterval cooldown between consecutive expiration runs
 */
public record SnapshotExpirationPolicy(
    Duration retentionPeriod,
    int retainCount,
    Duration expirationInterval
) {
    public static SnapshotExpirationPolicy defaults() {
        return new SnapshotExpirationPolicy(
            Duration.ofDays(7),
            100,
            Duration.ofHours(12)
        );
    }
}
