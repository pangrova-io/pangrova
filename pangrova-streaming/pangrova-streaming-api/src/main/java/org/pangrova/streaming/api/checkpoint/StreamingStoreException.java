package org.pangrova.streaming.api.checkpoint;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Thrown when a streaming state store cannot persist or load its state. */
public class StreamingStoreException extends PangrovaException {

    public StreamingStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.STREAMING_STORE_ERROR, message, cause);
    }
}
