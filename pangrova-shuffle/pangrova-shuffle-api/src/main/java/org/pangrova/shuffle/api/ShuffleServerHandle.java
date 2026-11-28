package org.pangrova.shuffle.api;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import java.util.List;

/**
 * Endpoint and partition credentials issued by direct serving for one task.
 *
 * <p>Downstream tasks use the endpoint and opaque partition tickets to connect as
 * exchange sources. No native executor pointer crosses this contract.</p>
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
