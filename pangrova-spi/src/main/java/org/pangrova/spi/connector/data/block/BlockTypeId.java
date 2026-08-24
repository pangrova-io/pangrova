package org.pangrova.spi.connector.data.block;

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
