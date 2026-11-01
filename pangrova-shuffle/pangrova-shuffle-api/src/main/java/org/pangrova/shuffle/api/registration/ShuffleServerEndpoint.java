package org.pangrova.shuffle.api.registration;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Endpoint descriptor for a remote shuffle server that holds shuffle partition data.
 *
 * @param host the remote shuffle server hostname
 * @param port the remote shuffle server gRPC port (must be positive)
 * @param partitionStart first owned partition, inclusive
 * @param partitionEnd first partition after this endpoint's range, exclusive
 * @param tlsIdentity expected peer identity; blank only for explicitly plaintext providers
 */
public record ShuffleServerEndpoint(
        String host, int port, int partitionStart, int partitionEnd, String tlsIdentity) {

    private static final int MAX_PORT = 65_535;

    public ShuffleServerEndpoint {
        checkNotNull(host, "host must not be null");
        checkNotNull(tlsIdentity, "tlsIdentity must not be null");
        host = host.trim();
        if (host.isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("port is out of range: " + port);
        }
        if (partitionStart < 0 || partitionEnd <= partitionStart) {
            throw new IllegalArgumentException(
                    "partition range must be non-empty and half-open");
        }
        tlsIdentity = tlsIdentity.trim();
    }

    /** Returns whether this endpoint owns the given partition. */
    public boolean containsPartition(int partitionId) {
        return partitionId >= partitionStart && partitionId < partitionEnd;
    }
}
