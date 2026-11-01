package org.pangrova.metadata.api.error.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetadataException;

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
