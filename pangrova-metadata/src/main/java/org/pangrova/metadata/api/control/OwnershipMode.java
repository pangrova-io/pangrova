package org.pangrova.metadata.api.control;

/**
 * How Pangrova relates to a table.
 *
 * <p>{@link #OBSERVED} is a live catalog view and is never persisted. Only
 * {@link #LINKED} and {@link #MANAGED} are control-plane records.</p>
 */
public enum OwnershipMode {
    /** Visible in the catalog, not adopted; read-only. */
    OBSERVED,
    /** Externally written table explicitly adopted for governance and restricted maintenance. */
    LINKED,
    /** Created or written by Pangrova; the only mode that permits DML and destructive DDL. */
    MANAGED
}
