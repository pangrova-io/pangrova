package org.pangrova.common.client.exception;

/**
 * Thrown when the server response cannot be parsed or violates the expected protocol contract.
 * Covers: malformed JSON, unexpected HTTP status codes, serialization/deserialization failures.
 */
public final class PangrovaProtocolException extends PangrovaClientException {

    private final int httpStatusCode;

    public PangrovaProtocolException(String errorCode, String message, int httpStatusCode, Throwable cause) {
        super(errorCode, message, cause);
        this.httpStatusCode = httpStatusCode;
    }

    public PangrovaProtocolException(String errorCode, String message, int httpStatusCode) {
        super(errorCode, message);
        this.httpStatusCode = httpStatusCode;
    }

    /**
     * HTTP status code from the response, or -1 if not applicable (e.g. serialization error).
     */
    public int httpStatusCode() {
        return httpStatusCode;
    }
}
