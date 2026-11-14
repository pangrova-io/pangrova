package org.pangrova.planner.runtimefilter;

/**
 * Configuration for Dynamic Partition Pruning (DPP).
 * Feature flag defaults to enabled.
 *
 * @param enabled whether DPP runtime filter generation is enabled (default: true)
 */
public record DynamicPartitionPruningConfig(boolean enabled) {

    /** DPP disabled. */
    public static final DynamicPartitionPruningConfig DISABLED = new DynamicPartitionPruningConfig(false);

    /** DPP enabled (default). */
    public static final DynamicPartitionPruningConfig ENABLED = new DynamicPartitionPruningConfig(true);

    /** Default configuration — DPP enabled. */
    public static final DynamicPartitionPruningConfig DEFAULT = ENABLED;
}
