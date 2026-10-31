package org.pangrova.coordination.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** JDBC coordination failure, distinct from lease contention or an absent owner. */
@Internal
public final class CoordinationStorageException extends PangrovaException {
    private static final long serialVersionUID = 1L;

    /** Preserves the existing client error code and the original database failure. */
    public CoordinationStorageException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
