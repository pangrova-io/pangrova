package org.pangrova.spi.connector.data.write.model;

import org.pangrova.spi.annotation.Experimental;

/**
 * Write mode for table writes.
 */
@Experimental
public enum WriteMode {
    APPEND,
    UPSERT,
    OVERWRITE
}
