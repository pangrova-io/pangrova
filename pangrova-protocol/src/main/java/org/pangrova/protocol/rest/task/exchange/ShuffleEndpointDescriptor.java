package org.pangrova.protocol.rest.task.exchange;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Producer endpoint returned after a Native shuffle task is admitted. */
public record ShuffleEndpointDescriptor(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("tls_identity") String tlsIdentity,
        @JsonProperty("partition_tickets") List<ShufflePartitionTicketDescriptor> partitionTickets) {

    private static final int MAX_PORT = 65_535;

    public ShuffleEndpointDescriptor {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("invalid port: " + port);
        }
        tlsIdentity = tlsIdentity == null ? "" : tlsIdentity.trim();
        partitionTickets = List.copyOf(
                partitionTickets == null ? List.of() : partitionTickets);
        if (partitionTickets.isEmpty()) {
            throw new IllegalArgumentException("partitionTickets must not be empty");
        }
    }

    /**
     * Resolves the signed ticket for one output partition.
     *
     * @param partitionId output partition identifier
     * @return the matching signed ticket
     * @throws IllegalArgumentException if the endpoint has no ticket for the partition
     */
    public ShufflePartitionTicketDescriptor ticketFor(int partitionId) {
        return partitionTickets.stream()
                .filter(ticket -> ticket.partitionId() == partitionId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No ticket for shuffle partition " + partitionId));
    }
}
