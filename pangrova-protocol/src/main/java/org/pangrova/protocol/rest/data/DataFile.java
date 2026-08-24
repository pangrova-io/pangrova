package org.pangrova.protocol.rest.data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a single data file produced by a write task.
 */
public record DataFile(
    @JsonProperty("path") String path,
    @JsonProperty("size_bytes") long sizeBytes,
    @JsonProperty("row_count") long rowCount,
    @JsonProperty("partition") String partition
) {}
