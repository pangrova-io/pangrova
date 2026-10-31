package org.pangrova.connector.spi.data.write.model;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Supported table formats.
 */
@Experimental
public enum TableFormat {
    ICEBERG,
    PAIMON
}
