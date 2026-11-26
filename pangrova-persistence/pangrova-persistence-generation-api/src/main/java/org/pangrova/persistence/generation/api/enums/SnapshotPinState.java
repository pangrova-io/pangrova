package org.pangrova.persistence.generation.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Durable state of one source snapshot pin intent. */
@Experimental
public enum SnapshotPinState {
    INTENDED,
    PINNED,
    RELEASING,
    RELEASED
}
