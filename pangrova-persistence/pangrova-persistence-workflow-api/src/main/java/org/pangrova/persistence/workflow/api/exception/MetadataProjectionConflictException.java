package org.pangrova.persistence.workflow.api.exception;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;

/** Stale deployment or conflicting reuse of an operation ID; no new mutation was committed. */
@Experimental
public final class MetadataProjectionConflictException extends MetastoreException {
    public MetadataProjectionConflictException(String message) {
        super(message);
    }
}
