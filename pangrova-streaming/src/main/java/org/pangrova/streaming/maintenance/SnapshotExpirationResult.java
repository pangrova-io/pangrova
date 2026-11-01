package org.pangrova.streaming.maintenance;

/**
 * Result of a snapshot expiration operation.
 */
public record SnapshotExpirationResult(
    String tableName,
    int snapshotsExpired
) {}
