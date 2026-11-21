package org.pangrova.streaming.maintenance;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * SPI for table maintenance operations.
 */
public interface TableMaintenance {

    CompletableFuture<CompactionResult> compactAsync(String tableName);

    CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan);

    CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan);

    MaintenanceStatus getMaintenanceStatus(String tableName);

    /**
     * Returns whether a scheduled maintenance cycle has work to do for this table.
     *
     * <p>The status-based thresholds cover compaction pressure. Implementations
     * that own retention policy override this so periodic scans also reach
     * snapshot expiration and orphan cleanup even when no small-file threshold
     * is crossed.</p>
     */
    default boolean isScheduledMaintenanceDue(String tableName) {
        return false;
    }

    /**
     * Runs every policy-authorized operation that is due for this table.
     *
     * <p>Schedulers call this for periodic scans so a single table visit can
     * compact, expire snapshots, and clean orphan files under one lease and one
     * recorded history entry per operation. The default keeps compaction-only
     * implementations working.</p>
     */
    default CompletableFuture<Void> runScheduledMaintenanceAsync(String tableName) {
        return compactAsync(tableName).thenApply(ignored -> null);
    }
}
