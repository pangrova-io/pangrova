package org.pangrova.worker.result;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a result batch cannot be delivered to the output buffer.
 *
 * <p>Typical cause: the output buffer remained full after the backpressure
 * timeout expired, meaning the batch would be silently dropped. Throwing
 * this exception ensures the {@link ResultPuller} detects the failure and
 * propagates it to the task lifecycle instead of continuing with incomplete
 * results.</p>
 */
public final class BatchRejectedException extends PangrovaException {

    private final int batchSizeBytes;

    public BatchRejectedException(int batchSizeBytes, long timeoutMs) {
        super(PangrovaErrorCode.RUNTIME_RESOURCE_EXHAUSTED,
                "Output buffer full after " + timeoutMs + "ms timeout, batch dropped"
                        + " batchSizeBytes=" + batchSizeBytes);
        this.batchSizeBytes = batchSizeBytes;
    }

    /**
     * Returns the size of the rejected batch in bytes.
     */
    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }
}
