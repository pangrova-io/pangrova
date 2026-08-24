package org.pangrova.common.exception;

/**
 * Thrown when a CREATE STREAM is issued for a stream name that already exists
 * and IF NOT EXISTS was not specified.
 */
public final class StreamAlreadyExistsException extends PangrovaException {

    public StreamAlreadyExistsException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StreamAlreadyExistsException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
