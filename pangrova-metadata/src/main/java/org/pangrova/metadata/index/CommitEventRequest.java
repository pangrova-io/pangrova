package org.pangrova.metadata.index;

import java.time.Instant;

/**
 * Parameter object for recording a commit event in the shared index commit log.
 */
public record CommitEventRequest(
        String catalogName,
        String schemaName,
        String tableName,
        String snapshotId,
        String addedFilesJson,
        String removedFilesJson,
        String eventType,
        Instant createdAt
) {}
