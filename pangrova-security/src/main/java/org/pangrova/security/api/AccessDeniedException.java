package org.pangrova.security.api;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a user lacks the required privilege for an operation.
 * Stable SPI contract — used by AccessControl implementations.
 */
public class AccessDeniedException extends PangrovaException {

    public AccessDeniedException(String message) {
        super(PangrovaErrorCode.ACCESS_DENIED, message);
    }
}
