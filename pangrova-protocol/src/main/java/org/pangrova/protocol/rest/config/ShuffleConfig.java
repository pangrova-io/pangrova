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
     * {@code serdeFormat} defaults to "ARROW_BATCH"; {@code compressionCodec} defaults
     * to "lz4"; {@code batchSize} defaults to 10000; {@code persistenceMode} defaults
     * to "MEMORY_ONLY".
     */
    public ShuffleConfig {
        serdeFormat = serdeFormat == null ? "ARROW_BATCH" : serdeFormat;
        compressionCodec = compressionCodec == null ? "lz4" : compressionCodec;
        if (!"ARROW_BATCH".equals(serdeFormat)
                || !List.of("none", "lz4", "zstd").contains(compressionCodec)) {
            throw new IllegalArgumentException("Shuffle requires the canonical batch format and compression codec");
        }
        if (batchSize < 0) throw new IllegalArgumentException("Shuffle batch size must not be negative");
        batchSize = batchSize == 0 ? 10000 : batchSize;
        persistenceMode = persistenceMode == null ? "MEMORY_ONLY" : persistenceMode;
        partitionKeyIndices = partitionKeyIndices == null ? List.of() : List.copyOf(partitionKeyIndices);
    }

    /**
     * Uses the canonical batch format with no partition binding.
     */
    public ShuffleConfig(String compressionCodec, int batchSize, String persistenceMode) {
        this(null, compressionCodec, batchSize, persistenceMode, 0, List.of());
    }

    /**
     * Uses the canonical batch format and in-memory persistence with no partition binding.
     */
    public ShuffleConfig(String compressionCodec, int batchSize) {
        this(null, compressionCodec, batchSize, "MEMORY_ONLY", 0, List.of());
    }
}
