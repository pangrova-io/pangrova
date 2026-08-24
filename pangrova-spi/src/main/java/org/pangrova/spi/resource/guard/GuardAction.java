package org.pangrova.spi.resource.guard;

import org.pangrova.spi.annotation.Experimental;

/**
 * Action the execution layer should take when a query is flagged by a
 * {@link RunawayQueryGuard}.
 */
@Experimental(reason = "G-RES-06: runaway query guard SPI")
public enum GuardAction {

    /** Cancel the query immediately with an error message. */
    KILL,

    /** Throttle the query (reduce its CPU/memory allocation) but keep it running. */
    THROTTLE,

    /** Log a warning but take no corrective action. */
    WARN
}
