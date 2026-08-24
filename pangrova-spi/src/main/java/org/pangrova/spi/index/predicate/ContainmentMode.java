package org.pangrova.spi.index.predicate;

import org.pangrova.spi.annotation.Experimental;

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
