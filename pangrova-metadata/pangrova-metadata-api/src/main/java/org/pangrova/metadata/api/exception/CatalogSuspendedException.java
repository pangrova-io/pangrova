package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when accessing a catalog that is in suspended state.
 */
public class CatalogSuspendedException extends MetadataException {

    public CatalogSuspendedException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_SUSPENDED, message);
    }

    public CatalogSuspendedException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_SUSPENDED, message, cause);
    }
}
