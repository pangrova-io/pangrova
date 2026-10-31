package org.pangrova.ai.control.api;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Persistence failure of the atomic AI catalog export and durable build association. */
public final class CatalogExportStorageException extends PangrovaException {
    private static final long serialVersionUID = 1L;

    /** Preserves the existing client error code while assigning the Java failure to its AI owner. */
    public CatalogExportStorageException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
