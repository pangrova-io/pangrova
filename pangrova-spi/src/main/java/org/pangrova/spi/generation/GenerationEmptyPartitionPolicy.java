package org.pangrova.spi.generation;

/** Explicit zero-row publication policy; omission is never interpreted as permission. */
public enum GenerationEmptyPartitionPolicy {
    FORBID,
    ALLOW_ZERO_ROWS
}
