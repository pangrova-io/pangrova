package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Explicit retention classification of an observed data reference, never derived from storage
 * medium or PUSH/PULL direction. A classification alone is not a decommission authorization:
 * the owner must also prove complete residency, current publication and reader protection.
 */
@Experimental(reason = "Shuffle residency and reader-lease integration are being consolidated")
public enum ShuffleRetentionPolicy {
    /** Data lifetime is tied to the producing process. */
    EPHEMERAL,
    /**
     * Data can outlive the producing process after a verified drain/handoff. Local disk alone
     * does not establish this classification; the serving path must remain reachable.
     */
    DETACHED,
    /**
     * Data retention is independent of the compute process. This does not by itself prove
     * survival of a storage-node failure, Coordinator recovery or authorization of a new reader.
     */
    PERSISTENT
}
