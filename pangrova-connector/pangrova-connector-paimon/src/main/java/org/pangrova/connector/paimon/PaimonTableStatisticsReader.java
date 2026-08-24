package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds {@link TableStatistics} for a Paimon table from snapshot summary
 * and optional table-level column statistics.
 */
final class PaimonTableStatisticsReader {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonTableStatisticsReader.class);

    private PaimonTableStatisticsReader() {}

    /** Builds table statistics for the given Paimon table. */
    static TableStatistics build(Table paimonTable) {
        long rowCount = extractRowCount(paimonTable);
        long dataSize = extractDataSize(paimonTable);
        Map<String, TableStatistics.ColumnStatistics> colStats = collectPaimonColumnStats(paimonTable);
        return new TableStatistics(rowCount, dataSize, colStats);
    }

    private static Map<String, TableStatistics.ColumnStatistics> collectPaimonColumnStats(
            Table paimonTable) {
        Map<String, TableStatistics.ColumnStatistics> colStats = new LinkedHashMap<>();
        RowType rowType = paimonTable.rowType();
        for (DataField field : rowType.getFields()) {
            colStats.put(field.name(), TableStatistics.ColumnStatistics.UNKNOWN);
        }
        if (!(paimonTable instanceof FileStoreTable fst)) {
            return colStats;
        }
        try {
            org.apache.paimon.stats.Statistics tableStats = fst.statistics().orElse(null);
            if (tableStats != null) {
                for (DataField field : rowType.getFields()) {
                    org.apache.paimon.stats.ColStats<?> cs = tableStats.colStats().get(field.name());
                    if (cs != null) {
                        long nullCount = cs.nullCount().orElse(-1L);
                        long ndv = cs.distinctCount().orElse(-1L);
                        Comparable<?> min = cs.min().orElse(null);
                        Comparable<?> max = cs.max().orElse(null);
                        double avgSize = cs.avgLen().isPresent() ? (double) cs.avgLen().getAsLong() : -1.0;
                        colStats.put(field.name(),
                                new TableStatistics.ColumnStatistics(nullCount, ndv, min, max, avgSize));
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("event=paimon_column_stats_fallback table={} error={}",
                    paimonTable.name(), e.getMessage());
        }
        return colStats;
    }

    private static long extractDataSize(Table paimonTable) {
        if (!(paimonTable instanceof FileStoreTable fst)) {
            return -1;
        }
        try {
            Snapshot snapshot = fst.snapshotManager().latestSnapshot();
            if (snapshot == null) {
                return -1;
            }
            // Paimon Snapshot may expose total file size via commitKind metadata
            return -1; // Paimon does not expose total file size in snapshot summary
        } catch (Exception ignored) {
            return -1;
        }
    }

    private static long extractRowCount(Table paimonTable) {
        if (!(paimonTable instanceof FileStoreTable fst)) {
            return -1;
        }
        try {
            Snapshot snapshot = fst.snapshotManager().latestSnapshot();
            if (snapshot == null) {
                return -1;
            }
            Long count = snapshot.totalRecordCount();
            return count != null ? count : -1;
        } catch (Exception ignored) {
            return -1;
        }
    }
}
