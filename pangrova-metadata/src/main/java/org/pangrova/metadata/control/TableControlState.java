package org.pangrova.metadata.control;

/** Lifecycle state of a persisted table-control record. */
public enum TableControlState {
    /** Record matches the catalog and may be used. */
    ACTIVE,
    /** The controlled table is no longer listed by the catalog; awaits confirmation. */
    STALE,
    /** The physical table was created but its control record could not be completed. */
    ORPHANED_MANAGED,
    /** Identity evidence changed; writes and maintenance are blocked pending review. */
    QUARANTINED,
    /** The control record was retired; history is retained for provenance. */
    DROPPED
}
