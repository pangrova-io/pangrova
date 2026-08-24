package org.pangrova.metadata.exception.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.exception.MetadataException;

/**
 * Thrown when attempting to create a catalog that already exists.
 */
public class CatalogAlreadyExistsException extends MetadataException {

    public CatalogAlreadyExistsException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_ALREADY_EXISTS, message);
    }

    public CatalogAlreadyExistsException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_ALREADY_EXISTS, message, cause);
    }
}
