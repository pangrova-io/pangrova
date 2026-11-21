package org.pangrova.planner.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a query is rejected due to throttling limits.
 */
public class ThrottleRejectedException extends PangrovaException {

    public ThrottleRejectedException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
