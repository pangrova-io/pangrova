package org.pangrova.protocol.rest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * REST DTO for shuffle configuration.
 *
 * <p>Independent REST DTO — {@code TaskRequestBuilder} handles
 * DTO-to-proto conversion following the same pattern as {@code WriteConfig}.</p>
 */
public record ShuffleConfig(
    @JsonProperty("serde_format") String serdeFormat,
    @JsonProperty("compression_codec") String compressionCodec,
    @JsonProperty("batch_size") int batchSize,
    @JsonProperty("persistence_mode") String persistenceMode,
    @JsonProperty("num_partitions") int numPartitions,
    @JsonProperty("partition_key_indices") List<Integer> partitionKeyIndices
) {

    /**
     * Compact constructor providing sensible defaults.
     * {@code serdeFormat} defaults to "PRESTO_PAGES"; {@code compressionCodec} defaults
     * to "lz4"; {@code batchSize} defaults to 10000; {@code persistenceMode} defaults
     * to "MEMORY_ONLY".
     */
    public ShuffleConfig {
        serdeFormat = serdeFormat == null ? "PRESTO_PAGES" : serdeFormat;
        compressionCodec = compressionCodec == null ? "lz4" : compressionCodec;
        batchSize = batchSize <= 0 ? 10000 : batchSize;
        persistenceMode = persistenceMode == null ? "MEMORY_ONLY" : persistenceMode;
        partitionKeyIndices = partitionKeyIndices == null ? List.of() : List.copyOf(partitionKeyIndices);
    }

    /**
     * Backward-compatible constructor without serdeFormat and partition fields.
     */
    public ShuffleConfig(String compressionCodec, int batchSize, String persistenceMode) {
        this(null, compressionCodec, batchSize, persistenceMode, 0, List.of());
    }

    /**
     * Backward-compatible constructor without serdeFormat and persistenceMode.
     */
    public ShuffleConfig(String compressionCodec, int batchSize) {
        this(null, compressionCodec, batchSize, "MEMORY_ONLY", 0, List.of());
    }
}
