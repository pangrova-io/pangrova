package org.pangrova.streaming.maintenance;

/**
 * Result of a compaction operation.
 */
public record CompactionResult(
    String tableName,
    int filesCompacted,
    int filesProduced,
    long bytesRewritten
) {}
