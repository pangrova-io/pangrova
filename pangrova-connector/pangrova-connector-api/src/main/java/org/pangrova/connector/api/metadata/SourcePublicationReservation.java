package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.Optional;

/** Durable immutable candidate allocation; the address is known even if its preparation acknowledgement is lost. */
@Experimental
public record SourcePublicationReservation(SourceTableState base, String executionId,
        String fingerprint, String manifestRelativePath, Optional<AcceptedSourceSnapshot> baseSnapshot) {
    public SourcePublicationReservation {
        java.util.Objects.requireNonNull(base);
        java.util.Objects.requireNonNull(baseSnapshot);
        if (executionId == null || executionId.isBlank() || fingerprint == null
                || !fingerprint.matches("[0-9a-f]{64}")
                || !manifestRelativePath.equals("_pangrova/prepared/" + fingerprint + ".manifest")
                || (base.version() == 0) != baseSnapshot.isEmpty()) {
            throw new IllegalArgumentException("Invalid source publication reservation");
        }
        baseSnapshot.ifPresent(snapshot -> {
            if (!snapshot.tableId().equals(base.tableId()) || snapshot.version() != base.version()
                    || !snapshot.tenant().equals(base.key().tenant())
                    || !snapshot.datasetUri().equals(base.datasetUri()) || snapshot.generation() != base.generation()
                    || !java.util.Arrays.equals(snapshot.schemaSha256(), base.schemaSha256())) {
                throw new IllegalArgumentException("Source reservation base membership differs");
            }
        });
    }
}
