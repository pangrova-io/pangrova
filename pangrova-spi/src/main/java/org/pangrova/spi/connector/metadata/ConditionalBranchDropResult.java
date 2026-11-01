package org.pangrova.spi.connector.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Outcome of a branch removal conditioned on its observed snapshot ID. */
@Experimental
public enum ConditionalBranchDropResult {
    DROPPED,
    REF_CHANGED,
    RETRYABLE_CONFLICT,
    COMMIT_STATE_UNKNOWN,
    FAILED
}
