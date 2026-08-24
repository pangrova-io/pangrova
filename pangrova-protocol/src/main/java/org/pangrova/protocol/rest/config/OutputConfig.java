package org.pangrova.protocol.rest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for task output buffering and client interaction.
 */
public record OutputConfig(
    @JsonProperty("ticket") String ticket,
    @JsonProperty("max_buffered_bytes") long maxBufferedBytes,
    @JsonProperty("max_buffered_batches") int maxBufferedBatches,
    @JsonProperty("client_idle_timeout_ms") long clientIdleTimeoutMs
) {}
