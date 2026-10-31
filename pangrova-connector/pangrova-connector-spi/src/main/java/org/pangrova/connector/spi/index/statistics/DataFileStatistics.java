package org.pangrova.connector.spi.index.statistics;

import java.util.List;

/**
 * Aggregated statistics for a single data file across all tracked columns.
 */
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
