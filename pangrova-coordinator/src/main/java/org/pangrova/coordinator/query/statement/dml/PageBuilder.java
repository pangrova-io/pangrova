package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.parser.ast.Expression;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.TypedRow;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds Page instances from expression lists for DML operations.
 */
final class PageBuilder {

    private PageBuilder() {}

    static Page buildPage(List<String> columns, List<List<Expression>> valueRows) {
        List<TypedRow> rows = valueRows.stream()
                .map(row -> buildRow(columns, row))
                .toList();
        return new Page(columns, rows);
    }

    private static TypedRow buildRow(List<String> columns, List<Expression> values) {
        if (columns.size() != values.size()) {
            throw new org.pangrova.common.exception.ValidationException(
                    org.pangrova.common.exception.PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Column count (" + columns.size() + ") does not match value count ("
                            + values.size() + ")");
        }
        Map<String, ColumnValue> row = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), DmlExpressionEvaluator.extractLiteralColumnValue(values.get(i)));
        }
        return TypedRow.of(row);
    }
}
