package org.pangrova.spi.transport;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a transport result stream exceeds the configured maximum byte limit.
 */
public class ResultTooLargeException extends PangrovaException {

    private final long maxBytes;
    private final long actualBytes;

    /** Creates an exception describing the configured and observed result sizes. */
    public ResultTooLargeException(long maxBytes, long actualBytes) {
        super(PangrovaErrorCode.TRANSPORT_RESULT_TOO_LARGE,
                "Result stream exceeded limit: " + actualBytes + " bytes > " + maxBytes + " bytes max");
        this.maxBytes = maxBytes;
        this.actualBytes = actualBytes;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getActualBytes() {
        return actualBytes;
    }
}
