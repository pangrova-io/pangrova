package org.pangrova.shuffle;

import java.util.Arrays;

/** Opaque producer-issued credential for reading one shuffle partition. */
public record ShufflePartitionTicket(
        int partitionId, byte[] signedTicket, long expiresAtEpochMs) {

    public ShufflePartitionTicket {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        if (signedTicket == null || signedTicket.length == 0) {
            throw new IllegalArgumentException("signedTicket must not be empty");
        }
        if (expiresAtEpochMs <= 0) {
            throw new IllegalArgumentException("expiresAtEpochMs must be positive");
        }
        signedTicket = Arrays.copyOf(signedTicket, signedTicket.length);
    }

    @Override
    public byte[] signedTicket() {
        return Arrays.copyOf(signedTicket, signedTicket.length);
    }
}
