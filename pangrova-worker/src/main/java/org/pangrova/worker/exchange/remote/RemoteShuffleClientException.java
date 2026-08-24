package org.pangrova.worker.exchange.remote;

/**
 * Exception thrown when a remote shuffle client operation fails.
 *
 * <p>Wraps gRPC transport errors, timeouts, and protocol violations
 * into a unified exception type for the remote shuffle exchange layer.</p>
 */
public final class RemoteShuffleClientException extends RuntimeException {

    public RemoteShuffleClientException(String message) {
        super(message);
    }

    public RemoteShuffleClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
