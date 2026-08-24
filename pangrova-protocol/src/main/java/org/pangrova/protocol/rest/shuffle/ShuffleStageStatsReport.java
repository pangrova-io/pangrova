package org.pangrova.protocol.rest.shuffle;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Worker-to-Coordinator shuffle stage statistics report.
 *
 * <p>Sent by Worker after a shuffle stage completes, carrying real partition-level
 * statistics for adaptive query execution decisions.</p>
 *
 * <h3>Version compatibility</h3>
 * <ul>
 *   <li>{@code @JsonIgnoreProperties(ignoreUnknown = true)} — unknown fields
 *       from newer senders are silently ignored during deserialization.</li>
 *   <li>Primitive fields default to {@code 0} when absent (Java default).</li>
 *   <li>{@code partitionStats} defaults to empty map when absent.</li>
 * </ul>
 *
 * @param queryId        query this report belongs to (mandatory)
 * @param stageId        shuffle stage identifier (mandatory)
 * @param partitionStats per-partition statistics keyed by partition index
 * @param shuffleBytes   total bytes shuffled across all partitions
 * @param shuffleRows    total rows shuffled across all partitions
 * @param timestampMs    epoch millis when the report was generated
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ShuffleStageStatsReport(
        @JsonProperty("query_id") String queryId,
        @JsonProperty("stage_id") String stageId,
        @JsonProperty("partition_stats") Map<Integer, PartitionStats> partitionStats,
        @JsonProperty("shuffle_bytes") long shuffleBytes,
        @JsonProperty("shuffle_rows") long shuffleRows,
        @JsonProperty("timestamp_ms") long timestampMs
) {

    /**
     * Compact constructor enforcing mandatory fields and defensive copy.
     */
    public ShuffleStageStatsReport {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (stageId == null || stageId.isBlank()) {
            throw new IllegalArgumentException("stageId must not be null or blank");
        }
        if (shuffleBytes < 0) {
            throw new IllegalArgumentException("shuffleBytes must not be negative, got: " + shuffleBytes);
        }
        if (shuffleRows < 0) {
            throw new IllegalArgumentException("shuffleRows must not be negative, got: " + shuffleRows);
        }
        partitionStats = partitionStats == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(partitionStats));
    }
}
