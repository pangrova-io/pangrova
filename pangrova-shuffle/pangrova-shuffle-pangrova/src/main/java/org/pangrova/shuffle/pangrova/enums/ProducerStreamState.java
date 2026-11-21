package org.pangrova.shuffle.pangrova.enums;

import org.pangrova.common.api.annotation.Internal;

/** Lifecycle of an individual producer RPC stream. */
@Internal
public enum ProducerStreamState {
    ACTIVE,
    FINISHING,
    FINISHED,
    ABORTED
}
