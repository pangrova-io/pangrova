package org.pangrova.metadata.exception.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.exception.MetadataException;

/**
 * Thrown when the specified catalog provider cannot be found.
 */
public class CatalogProviderNotFoundException extends MetadataException {

    public CatalogProviderNotFoundException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_PROVIDER_NOT_FOUND, message);
    }

    public CatalogProviderNotFoundException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_PROVIDER_NOT_FOUND, message, cause);
    }
}
