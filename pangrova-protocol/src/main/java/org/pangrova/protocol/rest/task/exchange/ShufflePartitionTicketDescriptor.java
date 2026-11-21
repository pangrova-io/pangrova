package org.pangrova.protocol.rest.task.exchange;

import com.fasterxml.jackson.annotation.JsonProperty;

/** JSON representation of one opaque producer-issued shuffle ticket. */
public record ShufflePartitionTicketDescriptor(
        @JsonProperty("partition_id") int partitionId,
        @JsonProperty("signed_ticket") String signedTicket,
        @JsonProperty("expires_at_epoch_ms") long expiresAtEpochMs) {

    public ShufflePartitionTicketDescriptor {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        if (signedTicket == null || signedTicket.isBlank()) {
            throw new IllegalArgumentException("signedTicket must not be blank");
        }
        if (expiresAtEpochMs <= 0) {
            throw new IllegalArgumentException("expiresAtEpochMs must be positive");
        }
    }
}
