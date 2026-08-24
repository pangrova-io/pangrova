package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a shuffle exchange source for inter-worker data transfer.
 *
 * <p>Sent from Coordinator to Worker as part of {@link UpdateExchangeSourcesRequest}
 * to inform the Worker where to pull upstream shuffle data via transport.</p>
 */
public record ExchangeSourceDescriptor(
    @JsonProperty("source_task_id") String sourceTaskId,
    @JsonProperty("host") String host,
    @JsonProperty("transport_port") int transportPort,
    @JsonProperty("partition_id") int partitionId,
    @JsonProperty("exchange_type") String exchangeType,
    @JsonProperty("sequence_id") long sequenceId,
    @JsonProperty("read_mode") String readMode
) {

    public ExchangeSourceDescriptor {
        if (sourceTaskId == null || sourceTaskId.isBlank()) {
            throw new IllegalArgumentException("sourceTaskId must not be blank");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (transportPort <= 0) {
            throw new IllegalArgumentException("transportPort must be > 0, got " + transportPort);
        }
    }

    /**
     * Backward-compatible constructor without readMode (defaults to BLOCKING).
     */
    public ExchangeSourceDescriptor(String sourceTaskId, String host, int transportPort,
                          int partitionId, String exchangeType, long sequenceId) {
        this(sourceTaskId, host, transportPort, partitionId, exchangeType, sequenceId, "BLOCKING");
    }
}
