package org.pangrova.planner.stats;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregated statistics for a single table.
 *
 * @param tableName            fully-qualified table name
 * @param columns              per-column statistics keyed by column name
 * @param lastUpdatedSnapshot  timestamp of the snapshot these stats were collected from
 */
public record PlannerTableStatistics(
        String tableName,
        Map<String, ColumnStatistics> columns,
        Instant lastUpdatedSnapshot) {

    public PlannerTableStatistics {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(columns, "columns must not be null");
        Objects.requireNonNull(lastUpdatedSnapshot, "lastUpdatedSnapshot must not be null");
        columns = Map.copyOf(columns);
    }
}
