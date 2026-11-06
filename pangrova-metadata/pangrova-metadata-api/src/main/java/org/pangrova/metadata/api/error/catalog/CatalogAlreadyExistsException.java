package org.pangrova.metadata.api.error.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetadataException;

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
