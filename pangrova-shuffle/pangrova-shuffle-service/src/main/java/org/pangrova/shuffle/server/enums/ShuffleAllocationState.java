package org.pangrova.shuffle.server.enums;

import org.pangrova.common.api.annotation.Internal;

/** Stored allocation lifecycle of the remote shuffle service. */
@Internal
public enum ShuffleAllocationState {
    REGISTERED,
    FINALIZED,
    CLEANING
}
