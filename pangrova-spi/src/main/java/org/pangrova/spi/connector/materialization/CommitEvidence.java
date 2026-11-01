package org.pangrova.spi.connector.materialization;

import java.time.Instant;
import org.pangrova.common.api.annotation.Stable;

/**
 * Exact, connector-typed evidence that a physical materialization was committed.
 *
 * <p>Evidence is the only token that authorizes publication. It is not a
 * boolean: it binds the connector identity, the exact commit fingerprint, the
 * manifest generation, and the connector format version so that a later
 * reconciliation can re-verify the commit after a crash without re-running the
 * data-plane work. An empty result still carries a real commit fingerprint.
 *
 * @param connectorId connector that committed the physical table
 * @param commitFingerprint connector-specific immutable commit identifier
 * @param manifestHash canonical hash of the committed manifest set
 * @param formatVersion connector commit format version
 * @param committedAt instant at which the connector observed the commit
 */
@Stable(since = "1.0")
public record CommitEvidence(
        String connectorId,
        String commitFingerprint,
        String manifestHash,
        String formatVersion,
        Instant committedAt) {

    public CommitEvidence {
        connectorId = requireText(connectorId, "connectorId");
        commitFingerprint = requireText(commitFingerprint, "commitFingerprint");
        manifestHash = requireText(manifestHash, "manifestHash");
        formatVersion = requireText(formatVersion, "formatVersion");
        if (committedAt == null) {
            throw new IllegalArgumentException("committedAt must not be null");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
