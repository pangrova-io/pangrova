package org.pangrova.materialization.enums;

import org.pangrova.common.api.annotation.Internal;

/** Result of reconciling a materialization attempt with its durable publication. */
@Internal
public enum ReconciliationOutcome {
    ADOPTED,
    SUPERSEDED,
    ABANDONED,
    QUARANTINED,
    DEFERRED
}
