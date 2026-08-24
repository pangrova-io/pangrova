package org.pangrova.governance.lineage;

import java.util.List;
import java.util.Set;

/**
 * Stored lineage record for a completed query execution.
 * Contains column-level derivation information and table references.
 *
 * @param queryId          unique query identifier
 * @param sql              original SQL text
 * @param columnDerivations column-level derivation mappings
 * @param sourceTables     fully qualified source table names
 * @param targetTables     fully qualified target table names (for DML)
 * @param timestampEpochMs epoch milliseconds when the query completed
 */
public record LineageRecord(
        String queryId,
        String sql,
        List<ColumnDerivationEntry> columnDerivations,
        Set<String> sourceTables,
        Set<String> targetTables,
        long timestampEpochMs
) {
    public LineageRecord {
        columnDerivations = columnDerivations == null ? List.of() : List.copyOf(columnDerivations);
        sourceTables = sourceTables == null ? Set.of() : Set.copyOf(sourceTables);
        targetTables = targetTables == null ? Set.of() : Set.copyOf(targetTables);
    }

    /**
     * Checks whether this record references a specific source column.
     *
     * @param tableName  fully qualified table name
     * @param columnName column name
     * @return true if any derivation references this source column
     */
    public boolean hasSourceColumn(String tableName, String columnName) {
        return columnDerivations.stream()
                .flatMap(d -> d.sourceColumns().stream())
                .anyMatch(sc -> sc.table().equals(tableName) && sc.column().equals(columnName));
    }

    /**
     * A single column derivation entry stored in the catalog.
     *
     * @param outputColumn  output column name
     * @param sourceColumns source columns contributing to this output
     * @param derivationType how the output is derived (DIRECT, TRANSFORM, AGGREGATE)
     */
    public record ColumnDerivationEntry(
            String outputColumn,
            List<SourceColumnRef> sourceColumns,
            String derivationType
    ) {
        public ColumnDerivationEntry {
            sourceColumns = sourceColumns == null ? List.of() : List.copyOf(sourceColumns);
        }
    }

    /**
     * Reference to a source column.
     *
     * @param table  fully qualified table name
     * @param column column name
     */
    public record SourceColumnRef(String table, String column) {}
}
