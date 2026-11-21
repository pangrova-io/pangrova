package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Base exception for domain-level (business logic) errors that are potentially
 * recoverable. Module-specific domain exceptions should extend this class to
 * enable catch-by-category error handling.
 *
 * <p>Examples of domain exceptions: parse errors, planner failures, metadata
 * resolution failures, throttle rejections, access denied.
 *
 * <p>This class is intentionally non-final so that modules can create their
 * own domain exception subtypes.
 *
 * @see InfrastructureException for system-level, usually non-recoverable errors
 * @see PangrovaException for the root of the Pangrova exception hierarchy
 */
public class DomainException extends PangrovaException {

    /**
     * Creates a domain exception with the given error code and message.
     *
     * @param errorCode structured error code for classification
     * @param message   human-readable error description
     */
    public DomainException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Creates a domain exception with the given error code, message, and cause.
     *
     * @param errorCode structured error code for classification
     * @param message   human-readable error description
     * @param cause     the underlying cause
     */
    public DomainException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
