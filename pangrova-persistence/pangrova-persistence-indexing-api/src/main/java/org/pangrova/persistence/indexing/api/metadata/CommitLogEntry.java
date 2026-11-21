package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Represents a single entry in the shared index commit log.
 */
@Experimental
public record CommitLogEntry(
        long eventId,
        String tenantId,
        String catalogName,
        String schemaName,
        String tableName,
        String snapshotId,
        String addedFilesJson,
        String removedFilesJson,
        String eventType,
        Instant createdAt,
        String commitFingerprint) {

    public CommitLogEntry {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
    }
}
