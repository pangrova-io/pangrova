package org.pangrova.metadata.index.model;

import java.time.Instant;

/**
 * Represents a single entry in the shared index commit log.
 */
public record CommitLogEntry(
        long eventId,
        String catalogName,
        String schemaName,
        String tableName,
        String snapshotId,
        String addedFilesJson,
        String removedFilesJson,
        String eventType,
        Instant createdAt
) {}
