package org.pangrova.common.client.exception;

/**
 * Thrown when the server reports an application-level error (HTTP 5xx or server-reported error codes).
 * The original server error code and message are preserved for client-side diagnostics.
 */
public final class PangrovaServerException extends PangrovaClientException {

    private final int httpStatusCode;

    public PangrovaServerException(String errorCode, String message, int httpStatusCode) {
        super(errorCode, message);
        this.httpStatusCode = httpStatusCode;
    }

    public PangrovaServerException(String errorCode, String message, int httpStatusCode, Throwable cause) {
        super(errorCode, message, cause);
        this.httpStatusCode = httpStatusCode;
    }

    /**
     * HTTP status code from the server response.
     */
    public int httpStatusCode() {
        return httpStatusCode;
    }
}
