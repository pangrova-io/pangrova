package org.pangrova.streaming.core.maintenance;

import java.time.Instant;

/**
 * Current maintenance status for a table.
 */
public record MaintenanceStatus(
    String tableName,
    int smallFilesCount,
    int totalFilesCount,
    long totalSizeBytes,
    int snapshotCount,
    int equalityDeleteFiles,
    long equalityDeleteBytes,
    long orphanFilesEstimate,
    Instant lastCompactionTime,
    Instant lastOrphanCleanupTime,
    Instant lastSnapshotExpireTime
) {}
