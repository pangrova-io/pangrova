package org.pangrova.persistence.catalog.api.exception;

import org.pangrova.metadata.api.exception.MetadataException;

import org.pangrova.common.enums.PangrovaErrorCode;

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
