package org.pangrova.planner.throttle;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a query is rejected due to throttling limits.
 */
public class ThrottleRejectedException extends PangrovaException {

    public ThrottleRejectedException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
