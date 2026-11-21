package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import org.pangrova.common.util.OrderedMaps;

/**
 * Strong-typed DTO representing split metadata for native engine serialization.
 *
 * <p>Replaces the previous {@code Map<String,Object>.toString() + Base64} approach
 * with structured JSON serialization, ensuring encode → decode idempotency and
 * full field semantics preservation across the Java ↔ Native boundary.</p>
 *
 * <p>Immutable record: all fields are final, collections are defensively copied.</p>
 *
 * @param splitId       unique identifier for this split within a task
 * @param tableName     fully qualified table name (catalog.schema.table)
 * @param partitionInfo partition column name-value pairs
 * @param filePath      data file path
 * @param offset        byte offset within the file
 * @param length        byte length of the split
 * @param properties    connector-specific properties (e.g., iceberg_info, paimon_info)
 */
public record SplitData(
        @JsonProperty("split_id") String splitId,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("partition_info") Map<String, String> partitionInfo,
        @JsonProperty("file_path") String filePath,
        @JsonProperty("offset") long offset,
        @JsonProperty("length") long length,
        @JsonProperty("properties") Map<String, String> properties
) {

    public SplitData {
        if (splitId == null || splitId.isBlank()) {
            throw new IllegalArgumentException("splitId must not be null or blank");
        }
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath must not be null or blank");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative, got: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative, got: " + length);
        }
        partitionInfo = copyOrdered(partitionInfo);
        properties = copyOrdered(properties);
    }

    private static Map<String, String> copyOrdered(Map<String, String> source) {
        return source == null
                ? Map.of()
                : OrderedMaps.immutableCopy(source);
    }
}
