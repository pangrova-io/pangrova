package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Parameter object for recording a commit event in the shared index commit log.
 */
@Experimental
public record CommitEventRequest(
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

    public CommitEventRequest(
            String tenantId, String catalogName, String schemaName, String tableName,
            String snapshotId, String addedFilesJson, String removedFilesJson,
            String eventType, Instant createdAt) {
        this(tenantId, catalogName, schemaName, tableName, snapshotId, addedFilesJson,
                removedFilesJson, eventType, createdAt, null);
    }

    public CommitEventRequest {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (commitFingerprint != null && !DigestUtils.isSha256Hex(commitFingerprint)) {
            throw new IllegalArgumentException("commitFingerprint must be a lowercase SHA-256 digest");
        }
    }
}
