package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.ColumnMetaData;

import java.util.List;

/**
 * Immutable result of a JDBC query execution, containing column metadata and row data.
 *
 * @param columns Avatica column metadata describing the result schema
 * @param rows    result rows as lists of values (one list per row)
 */
public record JdbcQueryResult(
        List<ColumnMetaData> columns,
        List<List<Object>> rows
) {
    public JdbcQueryResult {
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
    }
}
