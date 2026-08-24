package org.pangrova.common.exception;

/**
 * Thrown when an ingestion stream is referenced but does not exist
 * in the stream state store.
 */
public final class StreamNotFoundException extends PangrovaException {

    public StreamNotFoundException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StreamNotFoundException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
