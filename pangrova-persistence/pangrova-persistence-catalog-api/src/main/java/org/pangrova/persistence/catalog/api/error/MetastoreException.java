package org.pangrova.persistence.catalog.api.error;

import org.pangrova.metadata.api.error.MetadataException;

import org.pangrova.common.exception.PangrovaErrorCode;

/**
 * Thrown when metastore persistence operations fail.
 */
public class MetastoreException extends MetadataException {

    public MetastoreException(String message) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message);
    }

    public MetastoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
