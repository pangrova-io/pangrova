package org.pangrova.materialization.port;

import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.spi.connector.materialization.SourceRef;
import org.pangrova.spi.connector.materialization.SourceSnapshotRef;

/**
 * An acquired source pin whose owning attempt settled or whose hold expired.
 *
 * <p>Durable pin rows are written before the physical holds are acquired, so a
 * crash can leave a row without a matching provider hold. Recovery therefore
 * reconstructs the hold from durable state and relies on the documented
 * idempotency of {@link SourcePinning#unpin}.
 *
 * @param tenant tenant on whose behalf the source was read
 * @param attempt attempt that acquired the pin
 * @param source pinned source
 * @param snapshot pinned snapshot
 */
public record ReclaimableSourcePin(
        TenantId tenant,
        AttemptId attempt,
        SourceRef source,
        SourceSnapshotRef snapshot) {

    public ReclaimableSourcePin {
        if (tenant == null || attempt == null || source == null || snapshot == null) {
            throw new IllegalArgumentException("tenant, attempt, source, and snapshot are required");
        }
        if (!source.connectorId().equals(snapshot.connectorId())) {
            throw new IllegalArgumentException("snapshot connector does not match source connector");
        }
    }

    /** Returns the durable owner recorded when the hold was acquired. */
    public String pinOwner() {
        return attempt.value().toString();
    }
}
