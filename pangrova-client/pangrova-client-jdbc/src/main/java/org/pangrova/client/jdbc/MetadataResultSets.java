package org.pangrova.client.jdbc;

import org.pangrova.client.rest.PangrovaRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds the ResultSet payloads returned by {@link PangrovaDatabaseMetaData}
 * catalog-discovery queries: catalogs, schemas, tables, columns, and type info.
 */
final class MetadataResultSets {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataResultSets.class);

    private static final List<String> CATALOG_COLUMNS = Collections.singletonList("TABLE_CAT");
    private static final List<Integer> CATALOG_TYPES = Collections.singletonList(Types.VARCHAR);
    private static final List<String> SCHEMA_COLUMNS = List.of("TABLE_SCHEM", "TABLE_CATALOG");
    private static final List<Integer> SCHEMA_TYPES = List.of(Types.VARCHAR, Types.VARCHAR);
    private static final List<String> TABLE_COLUMNS = List.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS");
    private static final List<Integer> TABLE_TYPES = List.of(
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
    private static final List<String> COLUMN_COLUMNS = List.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
            "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "NULLABLE",
            "ORDINAL_POSITION", "IS_NULLABLE");
    private static final List<Integer> COLUMN_TYPES = List.of(
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.VARCHAR);
    /** Precision reported for the BIGINT type in getTypeInfo(). */
    private static final int BIGINT_PRECISION = 19;
    /** Precision reported for the DOUBLE type in getTypeInfo(). */
    private static final int DOUBLE_PRECISION = 15;

    private MetadataResultSets() {}

    /** Builds the getCatalogs() result set. */
    static ResultSet catalogs(PangrovaRestClient restClient) {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String catalog : restClient.listCatalogs()) {
                rows.add(new Object[]{catalog});
            }
            return new StaticResultSet(CATALOG_COLUMNS, CATALOG_TYPES, rows);
        } catch (Exception e) {
            LOG.warn("Failed to fetch catalogs", e);
            return empty(CATALOG_COLUMNS, CATALOG_TYPES);
        }
    }

    /** Builds the getSchemas() result set, optionally scoped to one catalog. */
    static ResultSet schemas(PangrovaRestClient restClient, String catalog) {
        try {
            List<Object[]> rows = new ArrayList<>();
            if (catalog != null && !catalog.isEmpty()) {
                // If catalog is specified, list tables in that catalog and extract schemas
                addSchemaRows(rows, restClient.listTables(catalog), catalog);
            } else {
                // List all catalogs and their tables
                for (String cat : restClient.listCatalogs()) {
                    addSchemaRows(rows, restClient.listTables(cat), cat);
                }
            }
            return new StaticResultSet(SCHEMA_COLUMNS, SCHEMA_TYPES, rows);
        } catch (Exception e) {
            LOG.warn("Failed to fetch schemas", e);
            return empty(SCHEMA_COLUMNS, SCHEMA_TYPES);
        }
    }

    /** Builds the getTables() result set with optional catalog/schema/name filters. */
    static ResultSet tables(PangrovaRestClient restClient, String catalog,
                            String schemaPattern, String tableNamePattern) {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String cat : catalogsToSearch(restClient, catalog)) {
                addTableRows(rows, restClient, cat, schemaPattern, tableNamePattern);
            }
            return new StaticResultSet(TABLE_COLUMNS, TABLE_TYPES, rows);
        } catch (Exception e) {
            LOG.warn("Failed to fetch tables", e);
            return empty(TABLE_COLUMNS, TABLE_TYPES);
        }
    }

    /** Builds the getColumns() result set with optional catalog/schema/table/column filters. */
    static ResultSet columns(PangrovaRestClient restClient, String catalog, String schemaPattern,
                             String tableNamePattern, String columnNamePattern) {
        try {
            List<Object[]> rows = new ArrayList<>();
            for (String cat : catalogsToSearch(restClient, catalog)) {
                addColumnRows(rows, restClient, cat, schemaPattern, tableNamePattern, columnNamePattern);
            }
            return new StaticResultSet(COLUMN_COLUMNS, COLUMN_TYPES, rows);
        } catch (Exception e) {
            LOG.warn("Failed to fetch columns", e);
            return empty(COLUMN_COLUMNS, COLUMN_TYPES);
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
        if (catalog != null && !catalog.isEmpty()) {
            return Collections.singletonList(catalog);
        }
        return restClient.listCatalogs();
    }

    private static void addSchemaRows(List<Object[]> rows,
                                      List<PangrovaRestClient.MetadataTableIdentifier> tables,
                                      String catalog) {
        for (var table : tables) {
            rows.add(new Object[]{schemaOf(table), catalog});
        }
    }

    private static void addTableRows(List<Object[]> rows, PangrovaRestClient restClient, String cat,
                                     String schemaPattern, String tableNamePattern) {
        try {
            for (var table : restClient.listTables(cat)) {
                addTableRow(rows, table, cat, schemaPattern, tableNamePattern);
            }
        } catch (Exception e) {
            LOG.debug("Failed to list tables in catalog {}", cat, e);
        }
    }

    private static void addTableRow(List<Object[]> rows, PangrovaRestClient.MetadataTableIdentifier table,
                                    String cat, String schemaPattern, String tableNamePattern) {
        if (!matchesPattern(table.name(), tableNamePattern)) {
            return;
        }
        String schema = schemaOf(table);
        if (!matchesPattern(schema, schemaPattern)) {
            return;
        }
        rows.add(new Object[]{cat, schema, table.name(), "TABLE", ""});
    }

    private static void addColumnRows(List<Object[]> rows, PangrovaRestClient restClient, String cat,
                                      String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        try {
            for (var table : restClient.listTables(cat)) {
                addTableColumnRows(rows, restClient, table, cat,
                        schemaPattern, tableNamePattern, columnNamePattern);
            }
        } catch (Exception e) {
            LOG.debug("Failed to load columns for catalog {}", cat, e);
        }
    }

    private static void addTableColumnRows(List<Object[]> rows, PangrovaRestClient restClient,
                                           PangrovaRestClient.MetadataTableIdentifier table, String cat,
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
        var tableInfo = restClient.loadTableMetadata(cat, table.name());
        if (tableInfo != null) {
            appendColumnRows(rows, tableInfo.columns(), cat, schema, table.name(), columnNamePattern);
        }
    }

    private static void appendColumnRows(List<Object[]> rows,
                                         List<PangrovaRestClient.MetadataColumnInfo> columns,
                                         String cat, String schema, String tableName,
                                         String columnNamePattern) {
        int position = 1;
        for (var col : columns) {
            if (!matchesPattern(col.name(), columnNamePattern)) {
                continue;
            }
            int sqlType = mapToSqlType(col.type());
            int nullable = col.required()
                    ? DatabaseMetaData.columnNoNulls
                    : DatabaseMetaData.columnNullable;
            rows.add(new Object[]{
                    cat, schema, tableName, col.name(),
                    sqlType, col.type(), 0, nullable,
                    position++, nullable == DatabaseMetaData.columnNoNulls ? "NO" : "YES"
            });
        }
    }

    private static String schemaOf(PangrovaRestClient.MetadataTableIdentifier table) {
        return table.namespace().isEmpty() ? "" : String.join(".", table.namespace());
    }

    private static boolean matchesPattern(String value, String pattern) {
        return pattern == null || pattern.isEmpty() || value.matches(pattern.replace("%", ".*"));
    }

    private static int mapToSqlType(String pangrovaType) {
        if (pangrovaType == null) {
            return Types.VARCHAR;
        }
        String lower = pangrovaType.toLowerCase();
        int numericType = mapNumericType(lower);
        if (numericType != Types.VARCHAR) {
            return numericType;
        }
        return mapTemporalOrBinaryType(lower);
    }

    private static int mapNumericType(String lower) {
        if (lower.contains("int")) {
            return Types.INTEGER;
        }
        if (lower.contains("long") || lower.contains("bigint")) {
            return Types.BIGINT;
        }
        if (lower.contains("float")) {
            return Types.FLOAT;
        }
        if (lower.contains("double")) {
            return Types.DOUBLE;
        }
        if (lower.contains("bool")) {
            return Types.BOOLEAN;
        }
        return Types.VARCHAR;
    }

    private static int mapTemporalOrBinaryType(String lower) {
        if (lower.contains("date")) {
            return Types.DATE;
        }
        if (lower.contains("timestamp")) {
            return Types.TIMESTAMP;
        }
        if (lower.contains("binary") || lower.contains("bytes")) {
            return Types.BINARY;
        }
        return Types.VARCHAR;
    }
}
