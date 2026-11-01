package org.pangrova.metadata.api.error;

import org.pangrova.common.exception.PangrovaErrorCode;

/**
 * Thrown when a referenced table does not exist.
 */
public class TableNotFoundException extends MetadataException {

    public TableNotFoundException(String message) {
        super(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND, message);
    }

    public TableNotFoundException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND, message, cause);
    }
}
