package org.pangrova.shuffle.exchange;

import java.util.Arrays;

/** Immutable producer endpoint bound to one upstream shuffle partition. */
public record ExchangeEndpoint(
        String sourceTaskId,
        int sourceStageId,
        int sourcePartitionId,
        int outputPartitionId,
        long planEpoch,
        int attemptId,
        long fencingToken,
        String host,
        int transportPort,
        String tlsIdentity,
        byte[] signedTicket,
        long ticketExpiresAtEpochMs,
        long expectedRows,
        long expectedBytes) {

    private static final int MAX_PORT = 65_535;

    public ExchangeEndpoint {
        requireNonBlank(sourceTaskId, "sourceTaskId");
        requireNonBlank(host, "host");
        validateIdentifiers(sourceStageId, sourcePartitionId, outputPartitionId, attemptId);
        validateEpochs(planEpoch, fencingToken);
        validatePort(transportPort);
        validateExpectedSizes(expectedRows, expectedBytes);
        validateExpiration(ticketExpiresAtEpochMs);
        tlsIdentity = tlsIdentity == null ? "" : tlsIdentity;
        signedTicket = copyRequiredTicket(signedTicket);
    }

    private static void validateIdentifiers(
            int sourceStageId, int sourcePartitionId, int outputPartitionId, int attemptId) {
        if (sourceStageId < 0 || sourcePartitionId < 0
                || outputPartitionId < 0 || attemptId < 0) {
            throw new IllegalArgumentException(
                    "partition and attempt identifiers must be non-negative");
        }
    }

    private static void validateEpochs(long planEpoch, long fencingToken) {
        if (planEpoch <= 0 || fencingToken <= 0) {
            throw new IllegalArgumentException(
                    "planEpoch and fencingToken must be positive");
        }
    }

    private static void validatePort(int transportPort) {
        if (transportPort <= 0 || transportPort > MAX_PORT) {
            throw new IllegalArgumentException("invalid transportPort: " + transportPort);
        }
    }

    private static void validateExpectedSizes(long expectedRows, long expectedBytes) {
        if (expectedRows < 0 || expectedBytes < 0) {
            throw new IllegalArgumentException("expected sizes must be non-negative");
        }
    }

    private static void validateExpiration(long ticketExpiresAtEpochMs) {
        if (ticketExpiresAtEpochMs <= 0) {
            throw new IllegalArgumentException(
                    "ticketExpiresAtEpochMs must be positive");
        }
    }

    private static byte[] copyRequiredTicket(byte[] signedTicket) {
        if (signedTicket == null || signedTicket.length == 0) {
            throw new IllegalArgumentException("signedTicket must not be empty");
        }
        return Arrays.copyOf(signedTicket, signedTicket.length);
    }

    @Override
    public byte[] signedTicket() {
        return Arrays.copyOf(signedTicket, signedTicket.length);
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
