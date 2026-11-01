package org.pangrova.spi.streaming.exception;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when durable streaming state persistence fails. */
public class StreamingStoreException extends PangrovaException {

    public StreamingStoreException(String message) {
        super(PangrovaErrorCode.STREAMING_STORE_ERROR, message);
    }

    public StreamingStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.STREAMING_STORE_ERROR, message, cause);
    }
}
