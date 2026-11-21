package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when a referenced catalog does not exist.
 */
public class CatalogNotFoundException extends MetadataException {

    public CatalogNotFoundException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_NOT_FOUND, message);
    }

    public CatalogNotFoundException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_NOT_FOUND, message, cause);
    }
}
