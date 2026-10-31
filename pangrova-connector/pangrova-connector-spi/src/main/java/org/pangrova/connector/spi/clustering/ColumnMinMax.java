package org.pangrova.connector.spi.clustering;

/**
 * Per-column min/max statistics for a data file.
 *
 * @param columnName column identifier
 * @param min        minimum value in the file for this column
 * @param max        maximum value in the file for this column
 */
public record ColumnMinMax(
        String columnName,
        Comparable<?> min,
        Comparable<?> max) {
}
