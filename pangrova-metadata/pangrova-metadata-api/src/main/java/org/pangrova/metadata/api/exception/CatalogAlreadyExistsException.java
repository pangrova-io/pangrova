package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

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
