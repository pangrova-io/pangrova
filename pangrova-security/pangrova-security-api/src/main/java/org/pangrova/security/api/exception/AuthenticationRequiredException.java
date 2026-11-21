package org.pangrova.security.api.exception;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when an operation requires an authenticated identity but none is present.
 * Maps to HTTP 401 Unauthorized at the REST layer.
 */
@Experimental
public class AuthenticationRequiredException extends PangrovaException {

    public AuthenticationRequiredException(String message) {
        super(PangrovaErrorCode.AUTHENTICATION_FAILED, message);
    }
}
