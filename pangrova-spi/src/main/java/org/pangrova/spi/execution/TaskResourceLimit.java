package org.pangrova.spi.execution;

/**
 * Resource limits for task execution.
 * SPI-level type — decoupled from protocol-layer ResourceLimit.
 *
 * @param maxMemoryBytes maximum memory allocation in bytes
 * @param maxThreads     maximum thread count
 * @param maxSpillBytes  maximum spill-to-disk bytes
 * @param spillDirectory spill directory path (empty string if unset)
 */
public record TaskResourceLimit(
        long maxMemoryBytes,
        int maxThreads,
        long maxSpillBytes,
        String spillDirectory) {

    /** Default resource limit with no constraints. */
    public static final TaskResourceLimit NONE = new TaskResourceLimit(0, 0, 0, "");
}
