package org.pangrova.protocol.rest.task.exchange;

import com.fasterxml.jackson.annotation.JsonProperty;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Complete wire binding for one producer of a typed ExchangeInput node. */
public record ExchangeSourceDescriptor(
        @JsonProperty("plan_node_id") String planNodeId,
        @JsonProperty("exchange_id") String exchangeId,
        @JsonProperty("expected_source_partitions") int expectedSourcePartitions,
        @JsonProperty("source_task_id") String sourceTaskId,
        @JsonProperty("source_stage_id") int sourceStageId,
        @JsonProperty("source_partition_id") int sourcePartitionId,
        @JsonProperty("output_partition_id") int outputPartitionId,
        @JsonProperty("plan_epoch") long planEpoch,
        @JsonProperty("attempt_id") int attemptId,
        @JsonProperty("fencing_token") long fencingToken,
        @JsonProperty("host") String host,
        @JsonProperty("transport_port") int transportPort,
        @JsonProperty("tls_identity") String tlsIdentity,
        @JsonProperty("signed_ticket") String signedTicket,
        @JsonProperty("ticket_expires_at_epoch_ms") long ticketExpiresAtEpochMs,
        @JsonProperty("expected_rows") long expectedRows,
        @JsonProperty("expected_bytes") long expectedBytes,
        @JsonProperty("read_mode") String readMode) {

    private static final int MAX_PORT = 65_535;

    public ExchangeSourceDescriptor {
        requireText(planNodeId, "planNodeId");
        requireText(exchangeId, "exchangeId");
        requireText(sourceTaskId, "sourceTaskId");
        requireText(host, "host");
        requireText(signedTicket, "signedTicket");
        validateExpectedPartitions(expectedSourcePartitions);
        validateIdentifiers(sourceStageId, sourcePartitionId, outputPartitionId, attemptId);
        validateEpochs(planEpoch, fencingToken);
        validatePort(transportPort);
        validateExpectedSizes(expectedRows, expectedBytes);
        validateExpiration(ticketExpiresAtEpochMs);
        tlsIdentity = tlsIdentity == null ? "" : tlsIdentity;
        readMode = readMode == null || readMode.isBlank() ? "BLOCKING" : readMode;
    }

    private static void validateExpectedPartitions(int expectedSourcePartitions) {
        if (expectedSourcePartitions <= 0) {
            throw new IllegalArgumentException(
                    "expectedSourcePartitions must be positive");
        }
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
}
