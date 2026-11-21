package org.pangrova.shuffle.pangrova.enums;

import org.pangrova.common.api.annotation.Internal;

/** Lifecycle of a task egress channel across its producer streams. */
@Internal
public enum TaskEgressState {
    ACTIVE,
    FINISHING,
    FINISHED,
    ABORTED
}
