package org.pangrova.databuild.api;

/** Explicit zero-row publication policy; omission is never interpreted as permission. */
public enum DataBuildEmptyPartitionPolicy {
    FORBID,
    ALLOW_ZERO_ROWS
}
