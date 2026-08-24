package org.pangrova.common.exception;

/**
 * Base exception for infrastructure-level (system) errors that are usually
 * non-recoverable. Module-specific infrastructure exceptions should extend
 * this class to enable catch-by-category error handling.
 *
 * <p>Examples of infrastructure exceptions: native bridge failures, exchange
 * connection errors, cache backend failures, circuit breaker open states,
 * no available worker conditions.
 *
 * <p>This class is intentionally non-final so that modules can create their
 * own infrastructure exception subtypes.
 *
 * @see DomainException for business-logic, potentially recoverable errors
 * @see PangrovaException for the root of the Pangrova exception hierarchy
 */
public class InfrastructureException extends PangrovaException {

    /**
     * Creates an infrastructure exception with the given error code and message.
     *
     * @param errorCode structured error code for classification
     * @param message   human-readable error description
     */
    public InfrastructureException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Creates an infrastructure exception with the given error code, message, and cause.
     *
     * @param errorCode structured error code for classification
     * @param message   human-readable error description
     * @param cause     the underlying cause
     */
    public InfrastructureException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
