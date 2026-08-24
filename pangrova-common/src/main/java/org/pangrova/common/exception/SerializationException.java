package org.pangrova.common.exception;

/**
 * Thrown when serialization or deserialization fails.
 */
public class SerializationException extends PangrovaException {

    public SerializationException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public SerializationException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
