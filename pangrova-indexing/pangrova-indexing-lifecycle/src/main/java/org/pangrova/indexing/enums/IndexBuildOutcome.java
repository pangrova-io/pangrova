package org.pangrova.indexing.enums;

/** Outcome of one index build attempt. */
public enum IndexBuildOutcome {
    COMPLETED,
    CLAIM_FAILED,
    STALE_GENERATION,
    REJECTED,
    CANCELLED
}
