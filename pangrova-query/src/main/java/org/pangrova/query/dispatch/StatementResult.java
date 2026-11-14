package org.pangrova.query.dispatch;

import org.pangrova.query.api.enums.StatementCategory;

import java.util.List;

import org.pangrova.types.value.TypedRow;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable result of a statement execution.
 * Rows and column names are defensively copied to prevent mutation.
 */
public record StatementResult(
        List<TypedRow> rows,
        List<String> columnNames,
        String mutatedTable,
        StatementCategory category) {

    public StatementResult {
        checkNotNull(category, "category");
        rows = rows != null ? List.copyOf(rows) : List.of();
        columnNames = columnNames != null ? List.copyOf(columnNames) : List.of();
    }
}
