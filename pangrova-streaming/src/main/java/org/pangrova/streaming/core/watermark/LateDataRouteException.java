package org.pangrova.streaming.core.watermark;

/**
 * Thrown when writing late data to a side-output table fails.
 */
public class LateDataRouteException extends RuntimeException {

    public LateDataRouteException(String message, Throwable cause) {
        super(message, cause);
    }
}
