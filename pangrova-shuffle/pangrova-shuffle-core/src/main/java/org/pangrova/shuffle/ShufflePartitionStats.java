package org.pangrova.shuffle;

/**
 * Partition-level shuffle statistics for AQE (Adaptive Query Execution).
 *
 * <p>Provides per-partition data size and row count collected after a shuffle
 * stage completes. Used by {@code PartitionCoalescer} and {@code SkewJoinHandler}
 * in the planner to make partition-aware optimization decisions.</p>
 *
 * <p>This record lives in {@code pangrova-spi} so that {@code pangrova-planner}
 * can reference it without depending on {@code pangrova-protocol}. The mapping
 * from protocol-layer {@code PartitionStats} to this SPI type happens at the
 * coordinator boundary.</p>
 *
 * @param partitionId the shuffle partition identifier
 * @param dataBytes   total data bytes written to this partition (must be non-negative)
 * @param rowCount    total row count in this partition (must be non-negative)
 */
public record ShufflePartitionStats(int partitionId, long dataBytes, long rowCount) {

    public ShufflePartitionStats {
        if (dataBytes < 0) {
            throw new IllegalArgumentException("dataBytes must be non-negative");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
    }
}
