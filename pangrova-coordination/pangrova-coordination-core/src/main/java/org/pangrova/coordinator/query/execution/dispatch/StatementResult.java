package org.pangrova.coordinator.query.execution.dispatch;

import java.util.List;

import org.pangrova.spi.connector.data.value.TypedRow;

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
