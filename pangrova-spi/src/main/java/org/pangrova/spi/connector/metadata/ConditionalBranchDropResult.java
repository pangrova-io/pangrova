package org.pangrova.spi.connector.metadata;

/** Outcome of a branch removal conditioned on its observed snapshot ID. */
public enum ConditionalBranchDropResult {
    DROPPED,
    REF_CHANGED,
    RETRYABLE_CONFLICT,
    COMMIT_STATE_UNKNOWN,
    FAILED
}
