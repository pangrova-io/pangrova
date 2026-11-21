package org.pangrova.planner.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception thrown when Substrait conversion fails (unsupported RelNode, type, or function).
 */
public class SubstraitConversionException extends PangrovaException {

    public SubstraitConversionException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public SubstraitConversionException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
