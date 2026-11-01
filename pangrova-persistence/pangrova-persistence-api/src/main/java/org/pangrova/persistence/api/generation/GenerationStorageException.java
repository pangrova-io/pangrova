package org.pangrova.persistence.api.generation;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Artifact persistence failure, distinct from stale ownership or revoked authorization. */
@Internal
public final class GenerationStorageException extends PangrovaException {
    private static final long serialVersionUID = 1L;

    /** Retains the existing persistence error code for clients while storage ownership moves to Persistence. */
    public GenerationStorageException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
