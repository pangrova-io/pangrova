package org.pangrova.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Handle returned by {@link ShuffleTransport#startServer} identifying
 * the local shuffle server endpoint for a task.
 *
 * <p>Downstream tasks use the host, port, and handle to connect as exchange sources.
 * The handle identifies the upstream executor within the multiplexed shuffle server,
 * enabling routing to the correct partition demuxer.</p>
 *
 * @param host           the shuffle server hostname
 * @param port           the shuffle server port (must be positive)
 * @param executorHandle the upstream executor handle for ticket routing
 */
public record ShuffleServerHandle(String host, int port, long executorHandle) {

    public ShuffleServerHandle {
        checkNotNull(host, "host must not be null");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive, got: " + port);
        }
    }
}
