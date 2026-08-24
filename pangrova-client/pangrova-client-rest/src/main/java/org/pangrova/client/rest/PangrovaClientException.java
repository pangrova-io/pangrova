package org.pangrova.client.rest;

/**
 * Unified exception for Pangrova client operations.
 * Carries an {@code errorCode} aligned with Pangrova server-side error semantics
 * (e.g. "QUERY_TIMEOUT", "INTERNAL_ERROR", "CONNECTION_FAILED").
 */
public class PangrovaClientException extends RuntimeException {

    private final String errorCode;

    public PangrovaClientException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public PangrovaClientException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String errorCode() {
        return errorCode;
    }
}
