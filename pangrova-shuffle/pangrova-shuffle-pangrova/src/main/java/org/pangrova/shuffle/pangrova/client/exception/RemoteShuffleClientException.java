package org.pangrova.shuffle.pangrova.client.exception;

import org.pangrova.shuffle.api.enums.ShuffleReadFailureReason;
import org.pangrova.shuffle.api.exception.ShuffleReadException;

/**
 * Exception thrown when a remote shuffle client operation fails.
 *
 * <p>Wraps gRPC transport errors, timeouts, and protocol violations
 * into a unified exception type for the remote shuffle exchange layer.</p>
 */
public final class RemoteShuffleClientException extends ShuffleReadException {

    public static RemoteShuffleClientException corruption(String detail) {
        return new RemoteShuffleClientException(ShuffleReadFailureReason.CORRUPTION,
                "External shuffle corruption: " + detail);
    }

    private RemoteShuffleClientException(ShuffleReadFailureReason reason, String message) {
        super(reason, message, null);
    }

    public RemoteShuffleClientException(String message) {
        super(ShuffleReadFailureReason.TRANSPORT, message, null);
    }

    public RemoteShuffleClientException(String message, Throwable cause) {
        super(ShuffleReadFailureReason.TRANSPORT, message, cause);
    }
}
