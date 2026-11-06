package org.pangrova.connector.api.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Specifies containment semantics for GIN index element queries.
 */
@Experimental
public enum ContainmentMode {

    /** At least one specified element must be present. */
    CONTAINS_ANY,

    /** All specified elements must be present. */
    CONTAINS_ALL
}
