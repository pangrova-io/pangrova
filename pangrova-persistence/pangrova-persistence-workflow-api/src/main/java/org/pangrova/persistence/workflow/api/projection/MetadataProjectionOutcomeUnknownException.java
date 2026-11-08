package org.pangrova.persistence.workflow.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import java.util.UUID;

/** Confirmation must query this operation ID; retrying with a new ID is unsafe. */
@Experimental
public final class MetadataProjectionOutcomeUnknownException extends MetastoreException {
    private final UUID operationId;

    public MetadataProjectionOutcomeUnknownException(UUID operationId, Throwable cause) {
        super("Metadata projection commit outcome is unknown: " + operationId, cause);
        this.operationId = java.util.Objects.requireNonNull(operationId, "operationId");
    }

    public UUID operationId() {
        return operationId;
    }
}
