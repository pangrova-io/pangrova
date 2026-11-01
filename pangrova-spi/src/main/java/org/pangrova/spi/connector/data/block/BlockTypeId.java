package org.pangrova.spi.connector.data.block;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Identifies the value type stored in a {@link Block}, enabling efficient
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
