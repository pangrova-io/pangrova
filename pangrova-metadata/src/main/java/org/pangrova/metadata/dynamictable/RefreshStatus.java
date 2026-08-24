package org.pangrova.metadata.dynamictable;

/**
 * Outcome status of a single Dynamic Table refresh execution.
 *
 * <p>Recorded in {@link RefreshHistoryEntry} for audit and diagnostics.</p>
 */
public enum RefreshStatus {

    /** Refresh completed successfully — incremental data was written. */
    SUCCESS,

    /** Refresh failed due to an error during detect, evaluate, or write. */
    FAILURE,

    /** No source changes detected — refresh skipped with no data written. */
    NO_OP,

    /** Refresh was cancelled before completion (e.g., HA failover or manual stop). */
    CANCELLED
}
