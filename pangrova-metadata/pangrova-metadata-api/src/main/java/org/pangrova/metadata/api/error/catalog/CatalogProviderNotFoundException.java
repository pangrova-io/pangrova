package org.pangrova.metadata.api.error.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetadataException;

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
