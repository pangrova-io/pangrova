package org.pangrova.persistence.coordination.api.exception;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Failure to obtain an authoritative coordination result, distinct from contention or absence.
 * A failed mutation may have committed before its acknowledgement was lost. Callers must confirm
 * current authority before retrying; this exception is never a grant, absence or successful release.
 * The cause is retained for diagnosis and is not part of a backend-independent wire contract.
 */
@Internal
public final class CoordinationStorageException extends PangrovaException {
    private static final long serialVersionUID = 1L;

    /** Preserves the existing client error code and the original backend failure. */
    public CoordinationStorageException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
