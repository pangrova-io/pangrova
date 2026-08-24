package org.pangrova.planner;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for planner-level errors (validation, optimization, statistics).
 */
public class PlannerException extends PangrovaException {

    public PlannerException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public PlannerException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
