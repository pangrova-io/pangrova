package org.pangrova.planner.core.substrait;

import org.pangrova.common.exception.PangrovaErrorCode;
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
