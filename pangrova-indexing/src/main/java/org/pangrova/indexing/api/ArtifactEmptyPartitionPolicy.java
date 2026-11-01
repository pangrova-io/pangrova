package org.pangrova.indexing.api;

/** Explicit zero-row publication policy; omission is never interpreted as permission. */
public enum ArtifactEmptyPartitionPolicy {
    FORBID,
    ALLOW_ZERO_ROWS
}
