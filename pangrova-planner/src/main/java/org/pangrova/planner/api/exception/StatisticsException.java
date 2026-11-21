package org.pangrova.planner.api.exception;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for statistics collection operations.
 */
@Internal
public class StatisticsException extends PangrovaException {

    public StatisticsException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StatisticsException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
