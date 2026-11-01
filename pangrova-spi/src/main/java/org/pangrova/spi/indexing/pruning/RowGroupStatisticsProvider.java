package org.pangrova.spi.indexing.pruning;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * SPI for reading per-row-group (Parquet) or per-stripe (ORC) column statistics
 * from data file footers.
 *
 * <p>Connector implementations read the file footer metadata to extract
 * min/max/null_count per row group for the requested columns. The coordinator
 * uses these statistics for sub-file pruning when file-level L0 stats return
 * UNKNOWN.</p>
 */
@Experimental
public interface RowGroupStatisticsProvider {

    /**
     * Returns per-row-group column statistics for the given data file and columns.
     *
     * @param dataFilePath path to the data file
     * @param columns      column names to collect statistics for
     * @return per-row-group statistics; empty list if not available
     */
    List<RowGroupPruningSummary> getRowGroupStatistics(
            String dataFilePath, List<String> columns);
}
