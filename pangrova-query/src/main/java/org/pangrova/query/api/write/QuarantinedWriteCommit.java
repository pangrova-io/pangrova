package org.pangrova.query.api.write;

import org.pangrova.common.api.annotation.Internal;

import java.time.Instant;
import java.util.Objects;

import static org.pangrova.common.validation.Preconditions.requireText;

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
        requireText(queryId, "queryId");
        requireText(commitFingerprint, "commitFingerprint");
        requireText(catalogName, "catalogName");
        requireText(tableName, "tableName");
        requireText(reason, "reason");
        Objects.requireNonNull(quarantinedAt, "quarantinedAt must not be null");
    }
}
