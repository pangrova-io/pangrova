package org.pangrova.streaming.maintenance;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * SPI for table maintenance operations.
 */
public interface TableMaintenanceSpi {

    CompletableFuture<CompactionResult> compactAsync(String tableName);

    CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan);

    CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan);

    MaintenanceStatus getMaintenanceStatus(String tableName);
}
