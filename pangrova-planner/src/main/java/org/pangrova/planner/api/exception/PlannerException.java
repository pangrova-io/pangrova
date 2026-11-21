package org.pangrova.planner.api.exception;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for planner-level errors (validation, optimization, statistics).
 */
@Internal
public class PlannerException extends PangrovaException {

    public PlannerException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public PlannerException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
