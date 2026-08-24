package org.pangrova.protocol.rest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Resource limits for task execution on a worker.
 */
public record ResourceLimitPayload(
    @JsonProperty("max_memory_bytes") long maxMemoryBytes,
    @JsonProperty("max_threads") int maxThreads,
    @JsonProperty("max_spill_bytes") long maxSpillBytes,
    @JsonProperty("spill_directory") String spillDirectory
) {}
