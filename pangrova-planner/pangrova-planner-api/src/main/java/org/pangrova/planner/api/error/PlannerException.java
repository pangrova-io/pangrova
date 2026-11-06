package org.pangrova.planner.api.error;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.common.exception.PangrovaErrorCode;
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
