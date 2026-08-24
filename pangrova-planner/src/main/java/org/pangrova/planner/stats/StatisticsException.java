package org.pangrova.planner.stats;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for statistics collection operations.
 */
public class StatisticsException extends PangrovaException {

    public StatisticsException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StatisticsException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
