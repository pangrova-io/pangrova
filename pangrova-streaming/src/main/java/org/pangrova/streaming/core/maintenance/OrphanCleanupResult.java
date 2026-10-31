package org.pangrova.streaming.core.maintenance;

/**
 * Result of an orphan file cleanup operation.
 */
public record OrphanCleanupResult(
    String tableName,
    int filesRemoved,
    long bytesReclaimed
) {}
