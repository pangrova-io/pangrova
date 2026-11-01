package org.pangrova.testing.integration.e2e.fixture;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Shared write helpers for E2E tests that insert typed rows into tables.
 *
 * <p>Provides batched row insertion, standard row generation, and SQL literal
 * formatting used across 11+ test files in write, connector, spill, cache,
 * resilience, udf, and io packages.</p>
 *
 * <p>The {@link #insertRows} method chunks large row lists into batches of
 * {@link #BATCH_SIZE} to avoid oversized SQL statements that exceed the
 * parser's capacity or cause excessive memory usage.</p>
 */
public final class WriteTestHelper {

    /** Default batch size for chunked inserts. */
    public static final int BATCH_SIZE = 500;

    private WriteTestHelper() {
        // utility class
    }

    /**
     * Insert rows into a table using batched INSERT statements.
     *
     * <p>Rows are chunked into groups of {@link #BATCH_SIZE}. Each chunk
     * is inserted as a single {@code INSERT INTO ... VALUES} statement.
     * Column names are derived from the first row's column set.</p>
     *
     * @param runner SQL test runner
     * @param fqn    fully qualified table name (catalog.schema.table)
     * @param rows   rows to insert; must not be empty
     */
    public static void insertRows(SqlTestRunner runner, String fqn, List<TypedRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            List<TypedRow> chunk = rows.subList(i, Math.min(i + BATCH_SIZE, rows.size()));
            insertChunk(runner, fqn, chunk);
        }
    }

    /**
     * Build standard test rows with columns: id (BIGINT), name (VARCHAR), amount (DOUBLE).
     *
     * <p>Each row has {@code id = n}, {@code name = "item_" + n},
     * {@code amount = n * 100.0} for {@code n} in {@code [startId, endId]}.</p>
     *
     * @param startId first row id (inclusive)
     * @param endId   last row id (inclusive)
     * @return list of typed rows
     */
    public static List<TypedRow> buildRows(int startId, int endId) {
        return LongStream.rangeClosed(startId, endId)
                .mapToObj(id -> TypedRow.builder()
                        .putRaw("id", id)
                        .putRaw("name", "item_" + id)
                        .putRaw("amount", (double) (id * 100))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Format a single row as a SQL VALUES tuple: {@code (val1, val2, ...)}.
     *
     * @param columns ordered column names
     * @param row     typed row to format
     * @return parenthesized, comma-separated SQL literal values
     */
    public static String toValuesTuple(List<String> columns, TypedRow row) {
        String values = columns.stream()
                .map(col -> toSqlLiteral(row.getRaw(col)))
                .collect(Collectors.joining(", "));
        return "(" + values + ")";
    }

    /**
     * Convert a Java value to a SQL literal string.
     *
     * <p>Handles {@code null} → {@code NULL}, {@link CharSequence} →
     * single-quoted with escaped inner quotes, and all other types
     * via {@link Object#toString()}.</p>
     *
     * @param value the value to convert; may be null
     * @return SQL literal representation
     */
    public static String toSqlLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof CharSequence) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        return value.toString();
    }

    private static void insertChunk(SqlTestRunner runner, String fqn, List<TypedRow> chunk) {
        if (chunk.isEmpty()) {
            return;
        }
        List<String> columns = List.copyOf(chunk.get(0).columnNames());
        String colList = String.join(", ", columns);
        String valuesCsv = chunk.stream()
                .map(row -> toValuesTuple(columns, row))
                .collect(Collectors.joining(", "));
        runner.execute(String.format("INSERT INTO %s (%s) VALUES %s", fqn, colList, valuesCsv));
    }
}
