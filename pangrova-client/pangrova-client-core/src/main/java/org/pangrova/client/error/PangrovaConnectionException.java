package org.pangrova.client.error;

/**
 * Thrown when a network-level error prevents communication with the Pangrova server.
 * Covers: connection refused, DNS resolution failure, network timeout, request interrupted.
 */
public final class PangrovaConnectionException extends PangrovaClientException {

    public PangrovaConnectionException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public PangrovaConnectionException(String errorCode, String message) {
        super(errorCode, message);
    }
}
