package org.pangrova.connector.spi.data.write.model;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Write mode for table writes.
 */
@Experimental
public enum WriteMode {
    APPEND,
    UPSERT,
    OVERWRITE
}
