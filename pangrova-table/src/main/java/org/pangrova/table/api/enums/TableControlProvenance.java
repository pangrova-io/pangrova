package org.pangrova.table.api.enums;

/** How a control record came into existence. */
public enum TableControlProvenance {
    /** Discovered in a catalog but not adopted. Never persisted. */
    DISCOVERED,
    /** An existing external table adopted explicitly. */
    ADOPTED,
    /** A table created by Pangrova. */
    CREATED
}
