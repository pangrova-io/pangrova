package org.pangrova.client.jdbc;

import org.pangrova.client.api.MetadataTableIdentifier;
import org.pangrova.client.api.MetadataColumnInfo;
import org.pangrova.transport.client.http.PangrovaRestClient;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Builds the ResultSet payloads returned by {@link PangrovaDatabaseMetaData}
 * catalog-discovery queries: catalogs, schemas, tables, columns, and type info.
 */
final class MetadataResultSets {

    private static final List<String> CATALOG_COLUMNS = Collections.singletonList("TABLE_CAT");
    private static final List<Integer> CATALOG_TYPES = Collections.singletonList(Types.VARCHAR);
    private static final List<String> SCHEMA_COLUMNS = List.of("TABLE_SCHEM", "TABLE_CATALOG");
    private static final List<Integer> SCHEMA_TYPES = List.of(Types.VARCHAR, Types.VARCHAR);
    private static final List<String> TABLE_COLUMNS = List.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM",
            "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
    private static final List<Integer> TABLE_TYPES = Collections.nCopies(TABLE_COLUMNS.size(), Types.VARCHAR);
    private static final List<String> COLUMN_COLUMNS = List.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
            "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
            "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
            "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN");
    private static final List<Integer> COLUMN_TYPES = List.of(
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
            Types.VARCHAR, Types.VARCHAR);
    /** Precision reported for the BIGINT type in getTypeInfo(). */
    private static final int BIGINT_PRECISION = 19;
    /** Precision reported for the DOUBLE type in getTypeInfo(). */
    private static final int DOUBLE_PRECISION = 15;

    private MetadataResultSets() {}

    /** Builds the getCatalogs() result set. */
    static ResultSet catalogs(PangrovaRestClient restClient) throws SQLException {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String catalog : restClient.listCatalogs()) {
                rows.add(new Object[]{catalog});
            }
            return new StaticResultSet(CATALOG_COLUMNS, CATALOG_TYPES, rows);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch catalogs", e);
        }
    }

    /** Builds the getSchemas() result set, optionally scoped to one catalog. */
    static ResultSet schemas(PangrovaRestClient restClient, String catalog, String schemaPattern) throws SQLException {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String cat : catalogsToSearch(restClient, catalog)) {
                for (String schema : restClient.listSchemas(cat)) {
                    if (matchesPattern(schema, schemaPattern)) rows.add(new Object[]{schema, cat});
                }
            }
            rows.sort(Comparator.comparing((Object[] row) -> (String) row[1]).thenComparing(row -> (String) row[0]));
            return new StaticResultSet(SCHEMA_COLUMNS, SCHEMA_TYPES, rows);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch schemas", e);
        }
    }

    /** Builds the getTables() result set with optional catalog/schema/name filters. */
    static ResultSet tables(PangrovaRestClient restClient, String catalog,
                            String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (types != null && java.util.Arrays.stream(types).noneMatch("TABLE"::equals)) {
            return empty(TABLE_COLUMNS, TABLE_TYPES);
        }
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String cat : catalogsToSearch(restClient, catalog)) {
                addTableRows(rows, restClient, cat, schemaPattern, tableNamePattern);
            }
            return new StaticResultSet(TABLE_COLUMNS, TABLE_TYPES, rows);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch tables", e);
        }
    }

    /** Builds the getColumns() result set with optional catalog/schema/table/column filters. */
    static ResultSet columns(PangrovaRestClient restClient, String catalog, String schemaPattern,
                             String tableNamePattern, String columnNamePattern) throws SQLException {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String cat : catalogsToSearch(restClient, catalog)) {
                addColumnRows(rows, restClient, cat, schemaPattern, tableNamePattern, columnNamePattern);
            }
            return new StaticResultSet(COLUMN_COLUMNS, COLUMN_TYPES, rows);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch columns", e);
        }
    }

    /** Builds the getTableTypes() result set listing the single supported type. */
    static ResultSet tableTypes() {
        return new StaticResultSet(Collections.singletonList("TABLE_TYPE"),
                Collections.singletonList(Types.VARCHAR),
                Collections.singletonList(new Object[]{"TABLE"}));
    }

    /** Builds the getTypeInfo() result set describing common SQL types. */
    static ResultSet typeInfo() {
        List<String> columnNames = List.of(
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
                "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
        );
        List<Integer> columnTypes = List.of(
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT,
                Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
                Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT
        );
        List<Object[]> rows = new ArrayList<>();

        // Add common SQL types
        rows.add(new Object[]{"VARCHAR", Types.VARCHAR, 0, "'", "'", null,
                DatabaseMetaData.typeNullable, true, DatabaseMetaData.typeSearchable,
                false, false, false, "VARCHAR", 0, 0, 0, 0, 10});
        rows.add(new Object[]{"INTEGER", Types.INTEGER, 10, null, null, null,
                DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable,
                true, false, false, "INTEGER", 0, 0, 0, 0, 10});
        rows.add(new Object[]{"BIGINT", Types.BIGINT, BIGINT_PRECISION, null, null, null,
                DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable,
                true, false, false, "BIGINT", 0, 0, 0, 0, 10});
        rows.add(new Object[]{"DOUBLE", Types.DOUBLE, DOUBLE_PRECISION, null, null, null,
                DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable,
                false, false, false, "DOUBLE", 0, 0, 0, 0, 10});
        rows.add(new Object[]{"BOOLEAN", Types.BOOLEAN, 1, null, null, null,
                DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable,
                false, false, false, "BOOLEAN", 0, 0, 0, 0, 10});

        return new StaticResultSet(columnNames, columnTypes, rows);
    }

    /** Builds an empty result set with the given column layout. */
    static ResultSet empty(List<String> columnNames, List<Integer> columnTypes) {
        return new StaticResultSet(columnNames, columnTypes, Collections.emptyList());
    }

    private static List<String> catalogsToSearch(PangrovaRestClient restClient, String catalog) {
        if (catalog == null) return restClient.listCatalogs();
        return catalog.isEmpty() ? List.of() : Collections.singletonList(catalog);
    }

    private static void addTableRows(List<Object[]> rows, PangrovaRestClient restClient, String cat,
                                     String schemaPattern, String tableNamePattern) {
        for (var table : tablesInCatalog(restClient, cat)) {
            addTableRow(rows, table, cat, schemaPattern, tableNamePattern);
        }
    }

    private static void addTableRow(List<Object[]> rows, MetadataTableIdentifier table,
                                    String cat, String schemaPattern, String tableNamePattern) {
        if (!matchesPattern(table.name(), tableNamePattern)) {
            return;
        }
        String schema = schemaOf(table);
        if (!matchesPattern(schema, schemaPattern)) {
            return;
        }
        rows.add(new Object[]{cat, schema, table.name(), "TABLE", "", null, null, null, null, null});
    }

    private static void addColumnRows(List<Object[]> rows, PangrovaRestClient restClient, String cat,
                                      String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        for (var table : tablesInCatalog(restClient, cat)) {
            addTableColumnRows(rows, restClient, table, cat,
                    schemaPattern, tableNamePattern, columnNamePattern);
        }
    }

    private static void addTableColumnRows(List<Object[]> rows, PangrovaRestClient restClient,
                                           MetadataTableIdentifier table, String cat,
                                           String schemaPattern, String tableNamePattern,
                                           String columnNamePattern) {
        if (!matchesPattern(table.name(), tableNamePattern)) {
            return;
        }
        String schema = schemaOf(table);
        if (!matchesPattern(schema, schemaPattern)) {
            return;
        }
        // Load table metadata to get columns
        var tableInfo = restClient.loadTableMetadata(table.namespace(), table.name());
        appendColumnRows(rows, tableInfo.columns(), cat, schema, table.name(), columnNamePattern);
    }

    private static void appendColumnRows(List<Object[]> rows,
                                         List<MetadataColumnInfo> columns,
                                         String cat, String schema, String tableName,
                                         String columnNamePattern) {
        int position = 1;
        for (var col : columns) {
            int ordinal = position++;
            if (!matchesPattern(col.name(), columnNamePattern)) {
                continue;
            }
            int sqlType = mapToSqlType(col.type());
            int nullable = col.required()
                    ? DatabaseMetaData.columnNoNulls
                    : DatabaseMetaData.columnNullable;
            rows.add(new Object[]{
                    cat, schema, tableName, col.name(),
                    sqlType, col.type(), columnSize(col.type(), sqlType), null, decimalDigits(col.type(), sqlType),
                    numericRadix(sqlType), nullable, null, null, null, null, null,
                    ordinal, nullable == DatabaseMetaData.columnNoNulls ? "NO" : "YES", null, null, null, null, "", ""
            });
        }
    }

    private static String schemaOf(MetadataTableIdentifier table) {
        return table.namespace().get(1);
    }

    private static List<MetadataTableIdentifier> tablesInCatalog(
            PangrovaRestClient client, String catalog) {
        var tables = client.listTables(catalog);
        for (var table : tables) {
            if (table.namespace().size() != 2 || !catalog.equals(table.namespace().get(0))) {
                throw new IllegalArgumentException("Table namespace does not match requested catalog");
            }
        }
        return tables.stream().sorted(Comparator.comparing(MetadataResultSets::schemaOf)
                .thenComparing(MetadataTableIdentifier::name)).toList();
    }

    private static boolean matchesPattern(String value, String pattern) {
        if (pattern == null) return true;
        var expression = new StringBuilder();
        boolean escaped = false;
        for (int codePoint : pattern.codePoints().toArray()) {
            String literal = new String(Character.toChars(codePoint));
            if (escaped) {
                expression.append(Pattern.quote(literal));
                escaped = false;
            } else if (codePoint == '\\') {
                escaped = true;
            } else if (codePoint == '%') {
                expression.append("(?s:.*)");
            } else if (codePoint == '_') {
                expression.append("(?s:.)");
            } else {
                expression.append(Pattern.quote(literal));
            }
        }
        if (escaped) expression.append(Pattern.quote("\\"));
        return value.matches(expression.toString());
    }

    private static int mapToSqlType(String pangrovaType) {
        String base = pangrovaType.trim().toUpperCase(Locale.ROOT).split("[<(]", 2)[0].trim();
        return switch (base) {
            case "BIGINT", "LONG", "I64" -> Types.BIGINT;
            case "SMALLINT", "I16" -> Types.SMALLINT;
            case "TINYINT", "I8" -> Types.TINYINT;
            case "INTEGER", "INT", "I32" -> Types.INTEGER;
            case "FLOAT", "FLOAT32", "REAL" -> Types.FLOAT;
            case "DOUBLE", "FLOAT64" -> Types.DOUBLE;
            case "DECIMAL", "NUMERIC" -> Types.DECIMAL;
            case "BOOL", "BOOLEAN" -> Types.BOOLEAN;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTAMP" -> Types.TIMESTAMP;
            case "BINARY", "BYTES", "VARBINARY" -> Types.BINARY;
            case "VARCHAR", "STRING", "TEXT" -> Types.VARCHAR;
            case "CHAR" -> Types.CHAR;
            case "ARRAY", "LIST", "VECTOR" -> Types.ARRAY;
            case "ROW", "STRUCT" -> Types.STRUCT;
            default -> Types.OTHER;
        };
    }

    private static Integer columnSize(String declared, int type) {
        var parameters = Pattern.compile("^[A-Za-z]+\\s*\\(\\s*(\\d+)(?:\\s*,\\s*(\\d+))?\\s*\\)$").matcher(declared);
        if ((type == Types.DECIMAL || type == Types.VARCHAR || type == Types.CHAR) && parameters.matches()) {
            return Integer.valueOf(parameters.group(1));
        }
        return switch (type) {
            case Types.TINYINT -> 3;
            case Types.SMALLINT -> 5;
            case Types.INTEGER -> 10;
            case Types.BIGINT -> BIGINT_PRECISION;
            case Types.BOOLEAN -> 1;
            default -> null;
        };
    }

    private static Integer decimalDigits(String declared, int type) {
        if (type != Types.DECIMAL) return null;
        var parameters = Pattern.compile("^[A-Za-z]+\\s*\\(\\s*\\d+\\s*,\\s*(\\d+)\\s*\\)$").matcher(declared);
        return parameters.matches() ? Integer.valueOf(parameters.group(1)) : null;
    }

    private static Integer numericRadix(int type) {
        return switch (type) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.DECIMAL -> 10;
            case Types.FLOAT, Types.DOUBLE -> 2;
            default -> null;
        };
    }
}
