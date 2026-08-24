package org.pangrova.testing.chaos.fault;

/**
 * Thrown when a fault injection operation fails.
 * Wraps the underlying Docker or container execution error with context about which fault was being injected.
 */
public final class FaultInjectionException extends RuntimeException {

    public FaultInjectionException(String message) {
        super(message);
    }

    public FaultInjectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
