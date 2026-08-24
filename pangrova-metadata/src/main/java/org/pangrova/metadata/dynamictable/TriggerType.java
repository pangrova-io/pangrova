package org.pangrova.metadata.dynamictable;

/**
 * Trigger source for a Dynamic Table refresh execution.
 *
 * <p>Used in {@link RefreshHistoryEntry} to distinguish how a refresh was initiated.</p>
 */
public enum TriggerType {

    /** Periodic refresh triggered by the scheduler based on target lag. */
    SCHEDULED,

    /** On-demand refresh triggered by ALTER DYNAMIC TABLE REFRESH. */
    MANUAL,

    /** Refresh triggered by an upstream Dynamic Table completing its refresh. */
    CASCADE
}
