package org.pangrova.connector.api.clustering;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Per-column min/max statistics for a data file.
 *
 * @param columnName column identifier
 * @param min        minimum value in the file for this column
 * @param max        maximum value in the file for this column
 */
@Experimental
public record ColumnMinMax(
        String columnName,
        Comparable<?> min,
        Comparable<?> max) {
}
