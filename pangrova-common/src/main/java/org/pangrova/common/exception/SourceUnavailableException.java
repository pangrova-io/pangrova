package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when a stream source (e.g. Kafka broker) is unreachable or unavailable.
 * The CDC pipeline can classify this for retry or circuit-breaking.
 */
public final class SourceUnavailableException extends PangrovaException {

    public SourceUnavailableException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public SourceUnavailableException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
