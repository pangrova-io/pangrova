package org.pangrova.persistence.generation.api.job;

/** Durable state of one source snapshot pin intent. */
public enum SnapshotPinState {
    INTENDED,
    PINNED,
    RELEASING,
    RELEASED
}
