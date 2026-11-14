package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.metadata.api.exception.MetadataException;

/**
 * Thrown when communication with the catalog backend fails.
 */
public class CatalogBackendException extends MetadataException {

    public CatalogBackendException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, message);
    }

    public CatalogBackendException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, message, cause);
    }
}
