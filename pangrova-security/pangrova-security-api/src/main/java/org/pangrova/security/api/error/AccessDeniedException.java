package org.pangrova.security.api.error;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a user lacks the required privilege for an operation.
 * Shared security contract used by access-control implementations.
 */
@Experimental
public class AccessDeniedException extends PangrovaException {

    public AccessDeniedException(String message) {
        super(PangrovaErrorCode.ACCESS_DENIED, message);
    }
}
