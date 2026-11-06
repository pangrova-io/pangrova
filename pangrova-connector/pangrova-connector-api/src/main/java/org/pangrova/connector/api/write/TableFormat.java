package org.pangrova.connector.api.write;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Supported table formats.
 */
@Experimental
public enum TableFormat {
    ICEBERG,
    PAIMON
}
