package org.pangrova.table.api.observation;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Durable record of one observed version transition for a controlled table.
 *
 * <p>This is an observation, not a commit. An external writer may have produced
 * the new snapshot; Pangrova must not treat it as one of its own write intents.</p>
 */
public record TableVersionObservation(
        UUID tableId,
        String versionToken,
        String previousVersionToken,
        String source,
        Instant observedAt) {

    public TableVersionObservation {
        Objects.requireNonNull(tableId, "tableId");
        versionToken = requireText(versionToken, "versionToken");
        source = requireText(source, "source");
        Objects.requireNonNull(observedAt, "observedAt");
    }
}
