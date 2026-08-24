package org.pangrova.metadata.exception.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.exception.MetadataException;

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
