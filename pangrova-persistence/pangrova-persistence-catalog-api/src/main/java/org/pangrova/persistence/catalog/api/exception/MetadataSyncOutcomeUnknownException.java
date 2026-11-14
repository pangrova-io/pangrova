package org.pangrova.persistence.catalog.api.exception;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import java.util.UUID;

/** COMMIT acknowledgment was lost. Confirm this run before starting another synchronization. */
public final class MetadataSyncOutcomeUnknownException extends MetastoreException {
    private final UUID runId;

    public MetadataSyncOutcomeUnknownException(UUID runId, Throwable cause) {
        super("Metadata sync completion outcome unknown: " + runId, cause);
        this.runId = runId;
    }

    public UUID runId() {
        return runId;
    }
}
