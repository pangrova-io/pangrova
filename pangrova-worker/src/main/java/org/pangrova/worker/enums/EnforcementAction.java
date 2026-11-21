package org.pangrova.worker.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Action the memory budget enforcer recommends when global pressure is detected.
 */
@Experimental(reason = "G-RES-07: unified memory budget enforcement SPI")
public enum EnforcementAction {

    /** No action required; memory is within acceptable bounds. */
    NONE,

    /** Spill in-memory data of the largest consumers to disk. */
    SPILL,

    /** Shrink native memory quota (send a quota-update to the native arbitrator). */
    SHRINK_NATIVE,

    /** Reclaim memory from the largest query (trigger spill + partial release). */
    RECLAIM,

    /** Kill the largest memory consumer to prevent OOM (last resort). */
    KILL_LARGEST
}
