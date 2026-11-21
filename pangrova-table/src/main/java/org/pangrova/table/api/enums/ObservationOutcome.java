package org.pangrova.table.api.enums;

/** Result of reconciling one controlled table against the connector's live state. */
public enum ObservationOutcome {
    /** The observed identity and version match the control record. */
    UNCHANGED,
    /** A new version token was recorded and the control revision advanced. */
    VERSION_CHANGED,
    /** The format identity no longer matches; the record is quarantined. */
    IDENTITY_CHANGED,
    /** The catalog no longer resolves the table; the record is stale. */
    TABLE_MISSING,
    /** The connector cannot report identity or version evidence. */
    UNSUPPORTED,
    /** The record is retired or was concurrently quarantined; nothing was written. */
    IGNORED
}
