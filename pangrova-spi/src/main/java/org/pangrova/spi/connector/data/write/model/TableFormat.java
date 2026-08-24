package org.pangrova.spi.connector.data.write.model;

import org.pangrova.spi.annotation.Experimental;

/**
 * Supported table formats.
 */
@Experimental
public enum TableFormat {
    ICEBERG,
    PAIMON
}
