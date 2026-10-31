package org.pangrova.connector.spi.data.block;

/**
 * Identifies the value type stored in a {@link Block}, enabling efficient
 * type dispatch without exception-based probing.
 */
public enum BlockTypeId {
    INT,
    LONG,
    DOUBLE,
    SLICE,
    ARRAY
}
