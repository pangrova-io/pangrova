package org.pangrova.spi.connector.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.List;

/**
 * SPI for ANALYZE TABLE statistics collection.
 *
 * <p>Connectors implement this to collect and persist table/column statistics
 * when the user runs {@code ANALYZE TABLE}. The engine invokes
 * {@link #collectTableStatistics} and feeds the result into the planner's
 * cost model.</p>
 *
 * <p>Implementations should use:</p>
 * <ul>
 *   <li>HyperLogLog for NDV (number of distinct values) estimation</li>
 *   <li>Equi-depth histograms (256 buckets) for value distribution</li>
 *   <li>Sampling when {@code samplePercent < 100} to reduce I/O</li>
 * </ul>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Trino:</b> {@code ANALYZE schema.table} collects NDV, min/max, null count
 *       per column; stored as Iceberg Puffin files or Delta statistics.</li>
 *   <li><b>Spark:</b> {@code ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c1, c2}
 *       collects histograms and NDV; stored in Hive metastore.</li>
 *   <li><b>Iceberg:</b> Puffin files hold blob-encoded statistics (theta sketches
 *       for NDV); manifest summaries provide min/max/null count.</li>
 * </ul>
 *
 * @see ConnectorMetadata#analyzeTable
 */
@Experimental(reason = "G-STAT-01: ANALYZE statistics collection SPI")
public interface AnalyzeStatisticsCollector {

    /**
     * Collects table and column statistics for the given table.
     *
     * @param session       the current session context
     * @param table         the table handle to analyze
     * @param columns       columns to analyze (empty = all columns)
     * @param samplePercent percentage of data to sample (100.0 = full scan)
     * @return collected statistics including per-column NDV, histogram, min/max, null count
     */
    TableStatistics collectTableStatistics(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<String> columns,
            double samplePercent);

    /**
     * Returns true if this collector supports histogram generation.
     * Default: false (basic min/max/NDV only).
     */
    default boolean supportsHistogram() {
        return false;
    }

    /**
     * Returns true if this collector supports NDV estimation via HyperLogLog or similar sketches.
     * Default: true.
     */
    default boolean supportsNdvEstimation() {
        return true;
    }
}
