package org.pangrova.materialization.api.domain;

import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.common.text.Utf8Order;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

/**
 * Exact set of source snapshots a generation was computed from.
 *
 * <p>The vector is part of generation identity: incremental refresh, rewrite
 * eligibility, and reconciliation all compare it. Every snapshot must be
 * pinnable; a connector that cannot pin snapshots cannot participate in
 * materialization at all, so there is no unpinned production path.
 *
 * @param snapshots source-to-snapshot mapping
 * @param canonicalVersion monotonically increasing vector version supplied by the planner
 */
public record SourceVector(Map<SourceRef, SourceSnapshotRef> snapshots, long canonicalVersion) {

    public SourceVector {
        if (snapshots == null) {
            throw new IllegalArgumentException("snapshots must not be null");
        }
        if (canonicalVersion < 0) {
            throw new IllegalArgumentException("canonicalVersion must not be negative");
        }
        TreeMap<SourceRef, SourceSnapshotRef> copy =
                new TreeMap<>(Comparator.comparing(SourceRef::canonicalName));
        for (Map.Entry<SourceRef, SourceSnapshotRef> entry : snapshots.entrySet()) {
            SourceRef source = entry.getKey();
            SourceSnapshotRef snapshot = entry.getValue();
            if (source == null || snapshot == null) {
                throw new IllegalArgumentException("source vector entries must be complete");
            }
            if (!source.connectorId().equals(snapshot.connectorId())) {
                throw new IllegalArgumentException(
                        "snapshot connector " + snapshot.connectorId()
                                + " does not match source connector " + source.connectorId());
            }
            if (!snapshot.pinnable()) {
                throw new IllegalArgumentException(
                        "source " + source.canonicalName() + " is not pinnable and cannot be materialized");
            }
            copy.put(source, snapshot);
        }
        snapshots = Map.copyOf(copy);
    }

    /**
     * Returns the snapshot pinned for a source.
     *
     * @param source source table
     * @return snapshot when the source is part of this vector
     */
    public Optional<SourceSnapshotRef> snapshotOf(SourceRef source) {
        return Optional.ofNullable(snapshots.get(source));
    }

    /**
     * Returns the canonical SHA-256 identity of this vector.
     *
     * @return lowercase hex fingerprint
     */
    public String canonicalHash() {
        StringBuilder canonical = new StringBuilder("vector:").append(canonicalVersion);
        snapshots.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(SourceRef::canonicalName, Utf8Order::compareUnsigned)))
                .forEach(entry -> {
                    SourceSnapshotRef snapshot = entry.getValue();
                    canonical.append('|')
                            .append(entry.getKey().canonicalName())
                            .append('@')
                            .append(snapshot.snapshotToken())
                            .append(':')
                            .append(snapshot.schemaFingerprint())
                            .append(':')
                            .append(snapshot.policyFingerprint());
                });
        return Sha256Digests.sha256Hex(canonical.toString());
    }

    /**
     * Returns whether both vectors contain the same sources and snapshots.
     *
     * @param other other vector
     * @return true when canonical versions and snapshot identities match
     */
    public boolean sameSnapshots(SourceVector other) {
        return other != null
                && canonicalVersion == other.canonicalVersion
                && canonicalHash().equals(other.canonicalHash());
    }
}
