package org.pangrova.governance.api.quality;

import java.util.List;

/**
 * Result of a quality rule SQL query execution.
 *
 * @param totalRows  total number of rows evaluated
 * @param failedRows number of rows that violated the quality rule
 * @param failingSamples sample of failing rows (column values as strings), capped for memory safety
 */
public record QualityQueryResult(
        long totalRows,
        long failedRows,
        List<List<String>> failingSamples
) {

    /** Maximum number of failing row samples to retain. */
    public static final int MAX_SAMPLES = 10;

    public QualityQueryResult {
        if (totalRows < 0) {
            throw new IllegalArgumentException("totalRows must be non-negative");
        }
        if (failedRows < 0) {
            throw new IllegalArgumentException("failedRows must be non-negative");
        }
        failingSamples = List.copyOf(failingSamples);
    }
}
