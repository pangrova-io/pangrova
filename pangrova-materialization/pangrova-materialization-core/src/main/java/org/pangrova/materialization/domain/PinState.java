package org.pangrova.materialization.domain;

/**
 * State of a source snapshot pin.
 */
public enum PinState {

    /** Snapshot is retained for an active attempt. */
    ACQUIRED,

    /** Snapshot was released after the attempt settled. */
    RELEASED,

    /** Snapshot was reclaimed after its expiry. */
    EXPIRED
}
