package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds {@link TableStatistics} for a Paimon table from snapshot summary
 * and optional table-level column statistics.
 */
final class PaimonTableStatisticsReader {

    private PaimonTableStatisticsReader() {}

    /** Builds table statistics for the given Paimon table. */
    static TableStatistics build(FileStoreTable table, Snapshot snapshot, org.apache.paimon.types.RowType schema) {
        if (snapshot == null) return new TableStatistics(0, 0);
        FileStoreTable selected = table.copyWithoutTimeTravel(
                Map.of("scan.snapshot-id", Long.toString(snapshot.id())));
        var columnStats = new LinkedHashMap<String, TableStatistics.ColumnStatistics>();
        var statistics = selected.statistics();
        for (var field : schema.getFields()) {
            var statistic = statistics.map(value -> value.colStats().get(field.name())).orElse(null);
            if (statistic == null) {
                columnStats.put(field.name(), TableStatistics.ColumnStatistics.UNKNOWN);
            } else {
                columnStats.put(field.name(), new TableStatistics.ColumnStatistics(
                        statistic.nullCount().orElse(-1L), statistic.distinctCount().orElse(-1L),
                        statistic.min().orElse(null), statistic.max().orElse(null),
                        statistic.avgLen().isPresent() ? statistic.avgLen().getAsLong() : -1.0));
            }
        }
        // The SDK summary has no total byte-size field. A physical record-count
        // estimate (including merge records) is not an exact COUNT result.
        Long rowCount = snapshot.totalRecordCount();
        return new TableStatistics(rowCount == null ? -1 : rowCount, -1, columnStats);
    }
}
