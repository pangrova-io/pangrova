package org.pangrova.streaming.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Thrown when a streaming state store cannot persist or load its state. */
public class StreamingStoreException extends PangrovaException {

    public StreamingStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.STREAMING_STORE_ERROR, message, cause);
    }
}
