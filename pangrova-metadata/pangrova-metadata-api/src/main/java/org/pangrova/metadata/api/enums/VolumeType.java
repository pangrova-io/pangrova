package org.pangrova.metadata.api.enums;

/** Storage lifecycle ownership of a catalog volume. */
public enum VolumeType {
    /** Catalog manages the storage lifecycle (create, delete on DROP). */
    MANAGED,
    /** Catalog only tracks the external location; storage is externally managed. */
    EXTERNAL
}
