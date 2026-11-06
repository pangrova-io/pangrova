package org.pangrova.coordinator.query.audit;

import org.pangrova.planner.api.lineage.ColumnDerivation;

import java.util.List;

/**
 * Captures table and column-level lineage from a compiled query plan.
 * Populated during query compilation, consumed on query FINISHED.
 */
public record QueryLineageInfo(
        String sql,
        List<String> sourceTables,
        List<String> targetTables,
        List<ColumnDerivation> columnDerivations
) {
    public QueryLineageInfo {
        sourceTables = sourceTables != null ? List.copyOf(sourceTables) : List.of();
        targetTables = targetTables != null ? List.copyOf(targetTables) : List.of();
        columnDerivations = columnDerivations != null ? List.copyOf(columnDerivations) : List.of();
    }

    /**
     * Backward-compatible constructor without column derivations.
     */
    public QueryLineageInfo(String sql, List<String> sourceTables, List<String> targetTables) {
        this(sql, sourceTables, targetTables, List.of());
    }
}
