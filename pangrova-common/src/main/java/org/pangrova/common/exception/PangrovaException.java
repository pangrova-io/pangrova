package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Base exception for all Pangrova errors. Carries an {@link PangrovaErrorCode}
 * for structured error classification across modules.
 */
public class PangrovaException extends RuntimeException {

    private final PangrovaErrorCode errorCode;

    public PangrovaException(PangrovaErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public PangrovaException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public PangrovaErrorCode getErrorCode() {
        return errorCode;
    }
}
