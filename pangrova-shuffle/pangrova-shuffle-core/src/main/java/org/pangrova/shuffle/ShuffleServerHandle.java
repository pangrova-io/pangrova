package org.pangrova.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.List;

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
 * @param tlsIdentity    certificate identity for mutual TLS, or empty for plaintext development mode
 * @param partitionTickets producer-issued opaque partition credentials
 */
public record ShuffleServerHandle(
        String host,
        int port,
        String tlsIdentity,
        List<ShufflePartitionTicket> partitionTickets) {

    public ShuffleServerHandle {
        checkNotNull(host, "host must not be null");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive, got: " + port);
        }
        tlsIdentity = tlsIdentity == null ? "" : tlsIdentity.trim();
        partitionTickets = List.copyOf(checkNotNull(
                partitionTickets, "partitionTickets must not be null"));
        if (partitionTickets.isEmpty()) {
            throw new IllegalArgumentException("partitionTickets must not be empty");
        }
    }
}
