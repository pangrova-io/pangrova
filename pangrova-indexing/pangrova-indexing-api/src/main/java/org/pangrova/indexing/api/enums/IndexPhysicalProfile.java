package org.pangrova.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Static physical execution family of an index type, not a persisted format or mutable capability registry.
 * Actual build support remains subject to the installed strategy or Native capability and its algorithm.
 */
@Experimental
public enum IndexPhysicalProfile {
    /** Per-file strategy payload used for conservative SQL split pruning. */
    SCALAR_FILE_PRUNING,

    /** Durable Native artifact used for vector or text retrieval. */
    NATIVE_RETRIEVAL
}
