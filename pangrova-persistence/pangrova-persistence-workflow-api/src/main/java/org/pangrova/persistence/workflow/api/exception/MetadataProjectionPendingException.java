package org.pangrova.persistence.workflow.api.exception;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import java.util.UUID;

/** A durable request still requires recovery. New operation IDs must not bypass it. */
@Experimental
public final class MetadataProjectionPendingException extends MetastoreException {
    private final UUID operationId;

    public MetadataProjectionPendingException(UUID operationId, Throwable cause) {
        super("Metadata projection requires recovery: " + operationId, cause);
        this.operationId = java.util.Objects.requireNonNull(operationId, "operationId");
    }

    public UUID operationId() {
        return operationId;
    }
}
