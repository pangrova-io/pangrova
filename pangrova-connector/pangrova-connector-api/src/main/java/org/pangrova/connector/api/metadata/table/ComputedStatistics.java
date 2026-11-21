package org.pangrova.connector.api.metadata.table;

import java.util.Map;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Statistics produced by an ANALYZE run and handed back to the connector to persist.
 *
 * <p>This is the write-side counterpart of {@link TableStatistics}: where
 * {@code getTableStatistics} reads statistics for the optimizer, {@code ComputedStatistics}
 * carries freshly-collected values from a {@code finishStatisticsCollection} call so the
 * connector can store them (e.g. as Iceberg Puffin blobs or a Paimon stats file).</p>
 *
 * @param rowCount    the collected row count, or -1 if not computed
 * @param columnStats per-column statistics keyed by column name; empty if none collected
 */
@Experimental
public record ComputedStatistics(long rowCount, Map<String, TableStatistics.ColumnStatistics> columnStats) {

    /** Empty result — nothing was collected. */
    public static final ComputedStatistics EMPTY = new ComputedStatistics(-1, Map.of());

    public ComputedStatistics {
        columnStats = columnStats == null ? Map.of() : Map.copyOf(columnStats);
    }

    /** Adapts a read-side {@link TableStatistics} snapshot into a collection payload. */
    public static ComputedStatistics from(TableStatistics stats) {
        return new ComputedStatistics(stats.rowCount(), stats.columnStats());
    }
}
