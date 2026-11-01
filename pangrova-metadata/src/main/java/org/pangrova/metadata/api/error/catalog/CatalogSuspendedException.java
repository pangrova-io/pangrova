package org.pangrova.metadata.api.error.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetadataException;

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
