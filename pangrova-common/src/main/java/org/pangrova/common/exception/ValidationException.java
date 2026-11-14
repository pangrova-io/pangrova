package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when input validation fails.
 */
public class ValidationException extends PangrovaException {

    public ValidationException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValidationException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
