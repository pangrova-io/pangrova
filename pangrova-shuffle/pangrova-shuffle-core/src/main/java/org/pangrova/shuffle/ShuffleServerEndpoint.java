package org.pangrova.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Endpoint descriptor for a remote shuffle server that holds shuffle partition data.
 *
 * @param host the remote shuffle server hostname
 * @param port the remote shuffle server gRPC port (must be positive)
 */
public record ShuffleServerEndpoint(String host, int port) {

    public ShuffleServerEndpoint {
        checkNotNull(host, "host must not be null");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive, got: " + port);
        }
    }
}
