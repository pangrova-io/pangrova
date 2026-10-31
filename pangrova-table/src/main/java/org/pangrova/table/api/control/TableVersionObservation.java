package org.pangrova.table.api.control;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

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
        versionToken = requireNonBlank(versionToken, "versionToken");
        source = requireNonBlank(source, "source");
        Objects.requireNonNull(observedAt, "observedAt");
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
