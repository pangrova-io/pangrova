package org.pangrova.shuffle.pangrova.client.exception;

/**
 * Exception thrown when a remote shuffle client operation fails.
 *
 * <p>Wraps gRPC transport errors, timeouts, and protocol violations
 * into a unified exception type for the remote shuffle exchange layer.</p>
 */
public final class RemoteShuffleClientException extends org.pangrova.shuffle.api.exception.ShuffleReadException {

    public static RemoteShuffleClientException corruption(String detail) {
        return new RemoteShuffleClientException(Reason.CORRUPTION, "External shuffle corruption: " + detail);
    }

    private RemoteShuffleClientException(Reason reason, String message) {
        super(reason, message, null);
    }

    public RemoteShuffleClientException(String message) {
        super(Reason.TRANSPORT, message, null);
    }

    public RemoteShuffleClientException(String message, Throwable cause) {
        super(Reason.TRANSPORT, message, cause);
    }
}
