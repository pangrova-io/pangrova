package org.pangrova.connector.api.metadata.table;

import java.util.Map;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable statistics for a connector table.
 *
 * @param rowCount      estimated number of rows, or -1 if unknown
 * @param dataSize      estimated data size in bytes, or -1 if unknown
 * @param sourceWorkUnits estimated number of independently scannable source units in the selected
 *                        snapshot, or -1 if unknown; zero denotes a proven empty source
 * @param columnStats   per-column statistics keyed by column name; empty if unavailable
 */
@Experimental
public record TableStatistics(long rowCount, long dataSize, long sourceWorkUnits,
        Map<String, ColumnStatistics> columnStats) {

    /** Constant representing unknown table statistics. */
    public static final TableStatistics UNKNOWN = new TableStatistics(-1, -1, -1, Map.of());

    /** Constructs statistics when source work-unit metadata is unavailable. */
    public TableStatistics(long rowCount, long dataSize, Map<String, ColumnStatistics> columnStats) {
        this(rowCount, dataSize, -1, columnStats);
    }

    /** Constructs statistics when only table cardinality and size are available. */
    public TableStatistics(long rowCount, long dataSize) {
        this(rowCount, dataSize, -1, Map.of());
    }

    public TableStatistics {
        if (sourceWorkUnits < -1) {
            throw new IllegalArgumentException("Unknown source work units use -1");
        }
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
