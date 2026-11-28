package org.pangrova.query.dispatch;

import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Map;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable result of a statement execution.
 * Declared column types take precedence over row-derived types.
 */
public record StatementResult(
        List<TypedRow> rows,
        List<String> columnNames,
        String mutatedTable,
        StatementCategory category,
        Map<String, PangrovaType> columnTypes) {

    public StatementResult(List<TypedRow> rows, List<String> columnNames, String mutatedTable,
                           StatementCategory category) {
        this(rows, columnNames, mutatedTable, category, Map.of());
    }

    /** Uses the first result row's column order for statements with a row-derived schema. */
    public static StatementResult fromRows(List<TypedRow> rows, String mutatedTable, StatementCategory category) {
        List<String> columns = rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
        return new StatementResult(rows, columns, mutatedTable, category);
    }

    public StatementResult {
        checkNotNull(category, "category");
        rows = rows != null ? List.copyOf(rows) : List.of();
        columnNames = columnNames != null ? List.copyOf(columnNames) : List.of();
        columnTypes = Map.copyOf(columnTypes);
        if (!columnNames.containsAll(columnTypes.keySet())) {
            throw new IllegalArgumentException("Declared column types must refer to result columns");
        }
    }
}
