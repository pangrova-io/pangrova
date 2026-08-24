package org.pangrova.spi.connector.metadata.table;

import java.util.Map;

import org.pangrova.spi.annotation.Experimental;

/**
 * Immutable statistics for a connector table.
 *
 * @param rowCount      estimated number of rows, or -1 if unknown
 * @param dataSize      estimated data size in bytes, or -1 if unknown
 * @param columnStats   per-column statistics keyed by column name; empty if unavailable
 */
@Experimental
public record TableStatistics(long rowCount, long dataSize, Map<String, ColumnStatistics> columnStats) {

    /** Constant representing unknown table statistics. */
    public static final TableStatistics UNKNOWN = new TableStatistics(-1, -1, Map.of());

    /** Backward-compatible constructor without column stats. */
    public TableStatistics(long rowCount, long dataSize) {
        this(rowCount, dataSize, Map.of());
    }

    public TableStatistics {
        columnStats = columnStats == null ? Map.of() : Map.copyOf(columnStats);
    }

    /** Per-column statistics from file metadata (Parquet/ORC footers). */
    public record ColumnStatistics(
            long nullCount,
            long ndv,
            Comparable<?> min,
            Comparable<?> max,
            double avgSize) {

        public static final ColumnStatistics UNKNOWN = new ColumnStatistics(-1, -1, null, null, -1.0);
    }
}
