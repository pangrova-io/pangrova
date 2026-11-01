package org.pangrova.spi.connector.clustering;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Per-file clustering statistics used for recluster planning.
 *
 * @param filePath    data file path
 * @param rowCount    number of rows in the file
 * @param columnStats per-column min/max statistics
 */
@Experimental
public record FileClusteringStats(
        String filePath,
        long rowCount,
        List<ColumnMinMax> columnStats) {

    public FileClusteringStats {
        columnStats = List.copyOf(columnStats);
    }
}
