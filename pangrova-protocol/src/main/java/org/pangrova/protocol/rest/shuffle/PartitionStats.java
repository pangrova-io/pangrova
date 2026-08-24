package org.pangrova.protocol.rest.shuffle;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-partition statistics collected after a shuffle stage completes.
 *
 * <p>Immutable record: all fields are final primitives with safe defaults
 * (zero) when absent during deserialization.</p>
 *
 * @param dataBytes number of bytes in this partition
 * @param rowCount  number of rows in this partition
 */
public record PartitionStats(
        @JsonProperty("data_bytes") long dataBytes,
        @JsonProperty("row_count") long rowCount
) {

    public PartitionStats {
        if (dataBytes < 0) {
            throw new IllegalArgumentException("dataBytes must not be negative, got: " + dataBytes);
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must not be negative, got: " + rowCount);
        }
    }
}
