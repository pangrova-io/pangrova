package org.pangrova.coordination.api.enums;

import org.pangrova.common.api.annotation.Stable;

/**
 * Singleton control-loop role for a coordinator in an active-active query cluster.
 *
 * <p>Role transitions are driven by the leader-election mechanism:
 * a coordinator starts as {@link #STANDBY}, transitions to {@link #LEADER} on promotion,
 * and returns to {@link #STANDBY} on demotion. {@link #STOPPED} indicates graceful
 * removal from the registry. Both LEADER and STANDBY coordinators accept and
 * schedule queries; the role controls only singleton metadata/background loops.</p>
 */
@Stable(since = "1.0")
public enum CoordinatorRole {

    /** This coordinator owns singleton control loops and also accepts query traffic. */
    LEADER,

    /** This coordinator accepts query traffic without owning singleton control loops. */
    STANDBY,

    /** This coordinator has been removed from the active pool (graceful shutdown). */
    STOPPED
}
