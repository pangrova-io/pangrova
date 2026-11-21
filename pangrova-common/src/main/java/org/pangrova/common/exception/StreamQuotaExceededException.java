package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when a per-tenant stream limit is exceeded during CREATE STREAM.
 */
public final class StreamQuotaExceededException extends PangrovaException {

    public StreamQuotaExceededException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public StreamQuotaExceededException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
