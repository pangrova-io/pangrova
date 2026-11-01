package org.pangrova.spi.connector.index.statistics;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Aggregated statistics for a single data file across all tracked columns.
 */
@Experimental
public record DataFileStatistics(
        String dataFilePath,
        String snapshotId,
        long rowCount,
        long fileSizeBytes,
        List<FileColumnStatistics> columnStatistics) {

    public DataFileStatistics {
        columnStatistics = List.copyOf(columnStatistics);
    }
}
