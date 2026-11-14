package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Base exception for all metadata-related errors.
 */
public class MetadataException extends PangrovaException {

    public MetadataException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public MetadataException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
