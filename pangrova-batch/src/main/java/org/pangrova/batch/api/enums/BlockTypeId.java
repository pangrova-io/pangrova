package org.pangrova.batch.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Identifies the value type stored in a {@link org.pangrova.batch.api.block.Block}, enabling efficient
 * type dispatch without exception-based probing.
 */
@Experimental
public enum BlockTypeId {
    INT,
    LONG,
    DOUBLE,
    SLICE,
    ARRAY
}
