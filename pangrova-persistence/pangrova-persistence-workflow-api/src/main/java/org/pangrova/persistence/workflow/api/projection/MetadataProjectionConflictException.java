package org.pangrova.persistence.workflow.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.persistence.catalog.api.error.MetastoreException;

/** Stale deployment or conflicting reuse of an operation ID; no new mutation was committed. */
@Experimental
public final class MetadataProjectionConflictException extends MetastoreException {
    public MetadataProjectionConflictException(String message) {
        super(message);
    }
}
