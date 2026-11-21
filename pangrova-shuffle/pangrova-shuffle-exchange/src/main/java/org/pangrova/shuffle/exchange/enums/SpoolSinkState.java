package org.pangrova.shuffle.exchange.enums;

import org.pangrova.common.api.annotation.Internal;

/** Write and terminal phases of the local exchange sink. */
@Internal
public enum SpoolSinkState {
    OPEN,
    FINISHED,
    ABORTED
}
