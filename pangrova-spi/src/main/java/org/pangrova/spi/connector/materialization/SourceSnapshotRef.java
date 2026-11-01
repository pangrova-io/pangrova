package org.pangrova.spi.connector.materialization;

import java.time.Instant;
import org.pangrova.common.api.annotation.Stable;

/**
 * Immutable identity of one observed source snapshot.
 *
 * <p>The schema and policy fingerprints bind a materialization result to the
 * exact source shape and authorization semantics it was computed against. A
 * materialization attempt that observes a different fingerprint must fail and
 * be re-planned instead of publishing a result with mismatched semantics.
 *
 * @param connectorId connector that produced the snapshot
 * @param snapshotToken connector-specific immutable snapshot identifier
 * @param schemaFingerprint canonical hash of the observed source schema
 * @param policyFingerprint canonical hash of the observed row-filter and column-mask policies
 * @param observedAt instant at which the snapshot was observed
 * @param pinnable whether the connector can retain this snapshot until unpinned
 */
@Stable(since = "1.0")
public record SourceSnapshotRef(
        String connectorId,
        String snapshotToken,
        String schemaFingerprint,
        String policyFingerprint,
        Instant observedAt,
        boolean pinnable) {

    /**
     * Token prefix for a source observed without any data snapshot.
     *
     * <p>The remainder of the token is connector-owned immutable evidence of
     * the empty state. An empty snapshot has no scan selector; it can only be
     * read in full.
     */
    public static final String EMPTY_TOKEN_PREFIX = "empty:";

    public SourceSnapshotRef {
        connectorId = requireText(connectorId, "connectorId");
        snapshotToken = requireText(snapshotToken, "snapshotToken");
        schemaFingerprint = requireText(schemaFingerprint, "schemaFingerprint");
        policyFingerprint = requireText(policyFingerprint, "policyFingerprint");
        if (observedAt == null) {
            throw new IllegalArgumentException("observedAt must not be null");
        }
    }

    /**
     * Returns whether the observed schema and policy fingerprints match another snapshot.
     *
     * @param other snapshot to compare against
     * @return true when both fingerprints are identical
     */
    public boolean hasSameShape(SourceSnapshotRef other) {
        return schemaFingerprint.equals(other.schemaFingerprint)
                && policyFingerprint.equals(other.policyFingerprint);
    }

    /**
     * Returns whether the source was observed without a data snapshot.
     *
     * @return true for an empty-source token
     */
    public boolean isEmptySource() {
        return snapshotToken.startsWith(EMPTY_TOKEN_PREFIX);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
