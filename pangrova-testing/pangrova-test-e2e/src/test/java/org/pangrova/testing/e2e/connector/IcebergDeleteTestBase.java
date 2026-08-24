package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base for Iceberg v2 delete tests (position delete and equality delete).
 *
 * <p>Configures an Iceberg v2 connector with {@code merge-on-read} delete mode,
 * which triggers PositionDeleteFilter / EqualityDeleteFilter in the native layer.
 *
 * <p>Provides shared helpers for table creation and row insertion so that
 * {@code RowDeleteByPositionTest} and {@code RowDeleteByKeyTest} stay focused
 * on delete-specific verification logic.
 *
 * <p>Requirements: 1.1-1.5, 2.1-2.5
 */
abstract class IcebergDeleteTestBase extends SqlE2ETestBase {

    protected static final String CATALOG = "iceberg_catalog";
    protected static final String SCHEMA = "default";
    protected static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    // -----------------------------------------------------------------------
    // Table creation helper
    // -----------------------------------------------------------------------

    /**
     * Creates an Iceberg v2 table with merge-on-read delete mode.
     *
     * @param table   unqualified table name
     * @param columns column definitions, e.g. "id BIGINT, name VARCHAR"
     */
    protected void createIcebergV2Table(String table, String columns) {
        String ddl = String.format(
                "CREATE TABLE %s%s (%s) TBLPROPERTIES ("
                        + "'format-version'='2', "
                        + "'write.delete.mode'='merge-on-read')",
                FQN_PREFIX, table, columns);
        sqlRunner.executeDdl(ddl);
    }

    // -----------------------------------------------------------------------
    // Row insertion helper
    // -----------------------------------------------------------------------

    /**
     * Inserts rows into the given table.
     *
     * <p>Each map entry represents a column name → value pair.
     * Values are formatted as SQL literals: strings are quoted,
     * nulls become {@code NULL}, numbers and booleans are unquoted.
     *
     * @param table unqualified table name
     * @param rows  non-empty list of column-value maps (all maps must share the same keys)
     */
    protected void insertRows(String table, List<TypedRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        List<String> columns = List.copyOf(rows.get(0).columnNames());
        String columnList = String.join(", ", columns);
        String valuesCsv = rows.stream()
                .map(row -> toValuesTuple(columns, row))
                .collect(Collectors.joining(", "));

        sqlRunner.execute(String.format(
                "INSERT INTO %s%s (%s) VALUES %s",
                FQN_PREFIX, table, columnList, valuesCsv));
    }

    // -----------------------------------------------------------------------
    // Fully-qualified name helper
    // -----------------------------------------------------------------------

    /** Returns the fully-qualified table name for queries. */
    protected String fqn(String table) {
        return FQN_PREFIX + table;
    }

    // -----------------------------------------------------------------------
    // Internal: SQL literal formatting
    // -----------------------------------------------------------------------

    private static String toValuesTuple(List<String> columns, TypedRow row) {
        String values = columns.stream()
                .map(col -> toSqlLiteral(row.getRaw(col)))
                .collect(Collectors.joining(", "));
        return "(" + values + ")";
    }

    private static String toSqlLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof CharSequence) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        return value.toString();
    }
}
