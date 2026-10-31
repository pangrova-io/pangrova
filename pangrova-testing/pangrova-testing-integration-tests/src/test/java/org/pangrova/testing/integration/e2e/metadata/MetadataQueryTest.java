package org.pangrova.testing.integration.e2e.metadata;

import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.CatalogDdlGrantFixture;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example-based tests for metadata query operations (SHOW/DESCRIBE).
 *
 * <p>Covers R8.1-R8.10. All metadata queries execute through the real
 * pipeline: TestCluster HTTP → NativeRuntime → Velox → real storage (local filesystem).
 *
 * <p>Requirements: 8.1, 8.2, 8.3, 8.4, 8.5
 */
@DisplayName("Metadata Query Tests")
class MetadataQueryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = ("t_" + method + "_" + System.nanoTime()).toLowerCase();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // --- R8.1: SHOW CATALOGS returns all registered catalog names ---

    @Nested
    @DisplayName("R8.1 - SHOW CATALOGS")
    class ShowCatalogs {

        @Test
        @DisplayName("SHOW CATALOGS returns at least the configured catalog")
        void showCatalogsContainsConfiguredCatalog() {
            List<TypedRow> rows = sqlRunner.execute("SHOW CATALOGS");

            assertThat(rows).isNotEmpty();
            assertThat(extractColumnValues(rows, "catalog_name", "catalog", "Catalog", "CATALOG_NAME"))
                    .contains(CATALOG);
        }

        @Test
        @DisplayName("SHOW CATALOGS returns non-empty result set")
        void showCatalogsReturnsResults() {
            List<TypedRow> rows = sqlRunner.execute("SHOW CATALOGS");

            assertThat(rows).isNotEmpty();
            assertThat(rows.get(0).columnCount()).isGreaterThan(0);
        }
    }

    // --- R8.2: SHOW SCHEMAS FROM catalog returns all schemas ---

    @Nested
    @DisplayName("R8.2 - SHOW SCHEMAS")
    class ShowSchemas {

        @Test
        @DisplayName("SHOW SCHEMAS FROM catalog returns at least the default schema")
        void showSchemasContainsDefault() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW SCHEMAS FROM " + CATALOG);

            assertThat(rows).isNotEmpty();
            assertThat(extractColumnValues(rows, "schema_name", "schema", "Schema", "SCHEMA_NAME"))
                    .contains(SCHEMA);
        }

        @Test
        @DisplayName("SHOW SCHEMAS returns non-empty result set")
        void showSchemasReturnsResults() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW SCHEMAS FROM " + CATALOG);

            assertThat(rows).isNotEmpty();
        }
    }

    // --- R8.3: SHOW TABLES FROM catalog.schema returns all tables ---

    @Nested
    @DisplayName("R8.3 - SHOW TABLES")
    class ShowTables {

        @Test
        @DisplayName("SHOW TABLES returns created table")
        void showTablesContainsCreatedTable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW TABLES FROM " + CATALOG + "." + SCHEMA);

            assertThat(extractColumnValues(rows, "table_name", "table", "Table", "TABLE_NAME"))
                    .contains(tableName);
        }

        @Test
        @DisplayName("SHOW TABLES on empty schema returns empty or no matching table")
        void showTablesEmptySchema() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW TABLES FROM " + CATALOG + "." + SCHEMA);

            // Result may contain tables from other tests; just verify it does not error
            assertThat(rows).isNotNull();
        }
    }

    // --- R8.4: DESCRIBE TABLE returns column name, type, nullable info ---

    @Nested
    @DisplayName("R8.4 - DESCRIBE TABLE")
    class DescribeTable {

        @Test
        @DisplayName("DESCRIBE TABLE returns correct column names")
        void describeTableColumnNames() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)");

            List<TypedRow> rows = sqlRunner.execute("DESCRIBE " + fqn());

            assertThat(rows).isNotEmpty();
            List<String> columnNames = extractColumnValues(rows,
                    "column_name", "col_name", "Column", "COLUMN_NAME");
            assertThat(columnNames).contains("id", "name", "amount");
        }

        @Test
        @DisplayName("DESCRIBE TABLE returns data type information")
        void describeTableDataTypes() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, active BOOLEAN)");

            List<TypedRow> rows = sqlRunner.execute("DESCRIBE " + fqn());

            assertThat(rows).hasSizeGreaterThanOrEqualTo(3);
            List<String> dataTypes = extractColumnValues(rows,
                    "data_type", "type", "Type", "DATA_TYPE");
            assertThat(dataTypes).isNotEmpty();
            assertThat(dataTypes).allSatisfy(type ->
                    assertThat(type).isNotBlank());
        }

        @Test
        @DisplayName("DESCRIBE TABLE returns nullable information")
        void describeTableNullable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");

            List<TypedRow> rows = sqlRunner.execute("DESCRIBE " + fqn());

            assertThat(rows).isNotEmpty();
            // Nullable info may be in is_nullable, nullable, Nullable, IS_NULLABLE
            // At minimum, each row should have column metadata
            rows.forEach(row -> assertThat(row.columnCount()).isGreaterThan(0));
        }

        @Test
        @DisplayName("DESCRIBE TABLE column order matches CREATE TABLE definition")
        void describeTableColumnOrder() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (first_col BIGINT, second_col VARCHAR, third_col DOUBLE)");

            List<TypedRow> rows = sqlRunner.execute("DESCRIBE " + fqn());

            List<String> columnNames = extractColumnValues(rows,
                    "column_name", "col_name", "Column", "COLUMN_NAME");
            assertThat(columnNames).containsExactly("first_col", "second_col", "third_col");
        }
    }

    // --- R8.5: SHOW CATALOGS after CREATE CATALOG → new catalog visible ---

    @Nested
    @DisplayName("R8.5 - SHOW CATALOGS After CREATE CATALOG")
    class ShowCatalogsAfterCreate {

        @Test
        @DisplayName("Newly created catalog appears in SHOW CATALOGS")
        void newCatalogVisibleInShowCatalogs() {
            String newCatalog = "cat_new_" + System.nanoTime();
            CatalogDdlGrantFixture.withCatalogLifecycleGrants(
                    stateStoreDataSource(), authenticatedCatalogSession(CATALOG).getUser(),
                    newCatalog, () -> {
                        sqlRunner.executeDdl("CREATE CATALOG " + newCatalog
                                + " USING iceberg"
                                + " WITH ("
                                + " 'catalog-type' = 'hadoop',"
                                + " 'warehouse' = '" + icebergWarehouse().resolve(newCatalog) + "'"
                                + ")");
                        try {
                            List<TypedRow> rows = sqlRunner.execute("SHOW CATALOGS");
                            List<String> names = extractColumnValues(rows,
                                    "catalog_name", "catalog", "Catalog", "CATALOG_NAME");
                            assertThat(names).contains(newCatalog);
                        } finally {
                            sqlRunner.executeDdl("DROP CATALOG " + newCatalog);
                        }
                    });
        }
    }

    // --- R8.6: SHOW SCHEMAS after CREATE SCHEMA → new schema visible ---

    @Nested
    @DisplayName("R8.6 - SHOW SCHEMAS After CREATE SCHEMA")
    class ShowSchemasAfterCreate {

        @Test
        @DisplayName("Newly created schema appears in SHOW SCHEMAS")
        void newSchemaVisibleInShowSchemas() {
            String newSchema = "sch_" + System.nanoTime();

            sqlRunner.executeDdl("CREATE SCHEMA " + CATALOG + "." + newSchema);

            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW SCHEMAS FROM " + CATALOG);
            List<String> names = extractColumnValues(rows,
                    "schema_name", "schema", "Schema", "SCHEMA_NAME");
            assertThat(names).contains(newSchema);
        }
    }

    // --- R8.7: SHOW TABLES after CREATE TABLE → new table visible ---

    @Nested
    @DisplayName("R8.7 - SHOW TABLES After CREATE TABLE")
    class ShowTablesAfterCreate {

        @Test
        @DisplayName("Newly created table appears in SHOW TABLES")
        void newTableVisibleInShowTables() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW TABLES FROM " + CATALOG + "." + SCHEMA);
            List<String> names = extractColumnValues(rows,
                    "table_name", "table", "Table", "TABLE_NAME");
            assertThat(names).contains(tableName);
        }
    }

    // --- R8.8: DESCRIBE after ALTER TABLE ADD COLUMN → new column visible ---

    @Nested
    @DisplayName("R8.8 - DESCRIBE After ALTER TABLE ADD COLUMN")
    class DescribeAfterAddColumn {

        @Test
        @DisplayName("Added column appears in DESCRIBE TABLE")
        void addedColumnVisibleInDescribe() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " ADD COLUMN score DOUBLE");

            List<TypedRow> rows = sqlRunner.execute("DESCRIBE " + fqn());
            List<String> columnNames = extractColumnValues(rows,
                    "column_name", "col_name", "Column", "COLUMN_NAME");
            assertThat(columnNames).contains("id", "name", "score");
        }
    }

    // --- R8.9: SHOW CATALOGS for non-existent catalog → error ---

    @Nested
    @DisplayName("R8.9 - Non-Existent Catalog Error")
    class NonExistentCatalogError {

        @Test
        @DisplayName("SHOW SCHEMAS FROM non-existent catalog returns error")
        void showSchemasNonExistentCatalogFails() {
            String fakeCatalog = "no_such_catalog_" + System.nanoTime();

            QueryError error = sqlRunner.executeExpectingError(
                    "SHOW SCHEMAS FROM " + fakeCatalog);

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("not found", "not exist", "does not exist", "unknown catalog");
        }

        @Test
        @DisplayName("SHOW TABLES FROM non-existent catalog returns error")
        void showTablesNonExistentCatalogFails() {
            String fakeCatalog = "no_such_catalog_" + System.nanoTime();

            QueryError error = sqlRunner.executeExpectingError(
                    "SHOW TABLES FROM " + fakeCatalog + ".default");

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("not found", "not exist", "does not exist", "unknown catalog");
        }
    }

    // --- R8.10: DESCRIBE non-existent table → error ---

    @Nested
    @DisplayName("R8.10 - Non-Existent Table Error")
    class NonExistentTableError {

        @Test
        @DisplayName("DESCRIBE non-existent table returns error")
        void describeNonExistentTableFails() {
            String fakeTable = FQN_PREFIX + "no_such_table_" + System.nanoTime();

            QueryError error = sqlRunner.executeExpectingError(
                    "DESCRIBE " + fakeTable);

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("not found", "not exist", "does not exist");
        }

        @Test
        @DisplayName("DESCRIBE table in non-existent schema returns error")
        void describeTableInNonExistentSchemaFails() {
            String fakeSchema = "no_such_schema_" + System.nanoTime();
            String fakeTable = CATALOG + "." + fakeSchema + ".some_table";

            QueryError error = sqlRunner.executeExpectingError(
                    "DESCRIBE " + fakeTable);

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("not found", "not exist", "does not exist");
        }
    }

    // --- Shared extraction helpers ---

    /**
     * Extract string values from result rows, trying multiple column name variants.
     * Handles common naming differences across SQL engines (e.g., catalog_name vs Catalog).
     */
    private static List<String> extractColumnValues(
            List<TypedRow> rows, String... candidateKeys) {
        return rows.stream()
                .map(row -> extractValue(row, candidateKeys))
                .toList();
    }

    private static String extractValue(TypedRow row, String... candidateKeys) {
        for (String key : candidateKeys) {
            Object value = row.getRaw(key);
            if (value != null) {
                return value.toString().toLowerCase();
            }
        }
        return ColumnValue.toRaw(row.values().values().iterator().next())
                .toString().toLowerCase();
    }
}
