package org.pangrova.spi.connector.data.write.model;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Supported table formats.
 */
@Experimental
public enum TableFormat {
    ICEBERG,
    PAIMON
}
