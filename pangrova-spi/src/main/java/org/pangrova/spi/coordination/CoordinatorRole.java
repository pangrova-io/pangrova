package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

/**
 * High-availability role for a coordinator instance in a multi-coordinator deployment.
 *
 * <p>Role transitions are driven by the leader-election mechanism:
 * a coordinator starts as {@link #STANDBY}, transitions to {@link #LEADER} on promotion,
 * and returns to {@link #STANDBY} on demotion. {@link #STOPPED} indicates graceful
 * removal from the registry.</p>
 */
@Stable(since = "1.0")
public enum CoordinatorRole {

    /** This coordinator is the active leader and accepts all query traffic. */
    LEADER,

    /** This coordinator is a hot-standby, ready to promote if the leader fails. */
    STANDBY,

    /** This coordinator has been removed from the active pool (graceful shutdown). */
    STOPPED
}
