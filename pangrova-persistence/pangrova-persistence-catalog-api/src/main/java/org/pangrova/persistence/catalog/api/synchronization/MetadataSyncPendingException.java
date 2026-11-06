package org.pangrova.persistence.catalog.api.synchronization;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import java.util.UUID;

/** A durable request still requires recovery. New operation IDs must not bypass it. */
@Experimental
public final class MetadataSyncPendingException extends MetastoreException {
    private final UUID runId;

    public MetadataSyncPendingException(UUID runId, Throwable cause) {
        super("Metadata sync completion requires recovery: " + runId, cause);
        this.runId = java.util.Objects.requireNonNull(runId, "runId");
    }

    public UUID runId() {
        return runId;
    }
}
