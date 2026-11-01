package org.pangrova.spi.connector.materialization;

import org.pangrova.common.api.annotation.Stable;

/**
 * A source together with the exact snapshot pinned for a materialization attempt.
 *
 * <p>The pin owner is the durable identity that acquired the pin, normally the
 * attempt identifier. It is part of the value so that concurrent attempts can
 * pin the same snapshot independently and release only their own hold.
 *
 * @param tenant tenant on whose behalf the source is read
 * @param source source table identity
 * @param snapshot pinned snapshot of that source
 * @param pinOwner durable identity that owns the pin
 */
@Stable(since = "1.0")
public record PinnedSource(
        String tenant, SourceRef source, SourceSnapshotRef snapshot, String pinOwner) {

    public PinnedSource {
        if (tenant == null || tenant.isBlank()) {
            throw new IllegalArgumentException("tenant must not be blank");
        }
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot must not be null");
        }
        if (pinOwner == null || pinOwner.isBlank()) {
            throw new IllegalArgumentException("pinOwner must not be blank");
        }
        if (!source.connectorId().equals(snapshot.connectorId())) {
            throw new IllegalArgumentException(
                    "snapshot connector " + snapshot.connectorId()
                            + " does not match source connector " + source.connectorId());
        }
    }
}
