package org.pangrova.connector.api.enums;

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
