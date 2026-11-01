package org.pangrova.spi.query.transaction;

import org.pangrova.common.api.annotation.Internal;

import java.time.Instant;
import java.util.Objects;

/**
 * A distributed write intent whose outcome could not be proven, retained for manual
 * reconciliation. The journal entry itself stays {@code PENDING}; this record is the
 * durable decision that recovery must not retry the exact fingerprint again.
 */
@Internal
public record QuarantinedWriteCommit(
        String queryId,
        String commitFingerprint,
        String catalogName,
        String tableName,
        String reason,
        Instant quarantinedAt) {

    public QuarantinedWriteCommit {
        requireNonBlank(queryId, "queryId");
        requireNonBlank(commitFingerprint, "commitFingerprint");
        requireNonBlank(catalogName, "catalogName");
        requireNonBlank(tableName, "tableName");
        requireNonBlank(reason, "reason");
        Objects.requireNonNull(quarantinedAt, "quarantinedAt must not be null");
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
