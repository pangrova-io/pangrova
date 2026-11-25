package org.pangrova.persistence.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Durable lifecycle states for an index schema-evolution operation. */
@Experimental
public enum IndexSchemaEvolutionState {
    INTENDED,
    COMMITTED,
    APPLIED,
    ABORTED
}
