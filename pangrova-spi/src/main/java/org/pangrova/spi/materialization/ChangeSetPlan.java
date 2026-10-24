package org.pangrova.spi.materialization;

import java.time.Instant;
import org.pangrova.spi.annotation.Stable;

/**
 * Connector-produced plan describing how to derive one snapshot from another.
 *
 * <p>The plan is opaque to the kernel. Its fingerprint is persisted with the
 * attempt so that a retry can be matched to the exact change interval and
 * strategy version that produced it.
 *
 * @param source source table the plan applies to
 * @param fromSnapshot base snapshot
 * @param toSnapshot target snapshot
 * @param mode change-set mode used by the plan
 * @param planFingerprint canonical hash of the connector plan
 * @param plannedAt instant at which the plan was produced
 */
@Stable(since = "1.0")
public record ChangeSetPlan(
        SourceRef source,
        SourceSnapshotRef fromSnapshot,
        SourceSnapshotRef toSnapshot,
        ChangeSetMode mode,
        String planFingerprint,
        Instant plannedAt) {

    public ChangeSetPlan {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (fromSnapshot == null || toSnapshot == null) {
            throw new IllegalArgumentException("fromSnapshot and toSnapshot must not be null");
        }
        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null");
        }
        if (planFingerprint == null || planFingerprint.isBlank()) {
            throw new IllegalArgumentException("planFingerprint must not be blank");
        }
        if (plannedAt == null) {
            throw new IllegalArgumentException("plannedAt must not be null");
        }
        if (!source.connectorId().equals(fromSnapshot.connectorId())
                || !source.connectorId().equals(toSnapshot.connectorId())) {
            throw new IllegalArgumentException("snapshot connector does not match source connector");
        }
        if (!fromSnapshot.hasSameShape(toSnapshot)) {
            throw new IllegalArgumentException(
                    "change-set endpoints must share schema and policy fingerprints");
        }
    }
}
