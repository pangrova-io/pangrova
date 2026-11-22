package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SDK initialization must inspect only the connection's exact catalog and schema. */
class IcebergJdbcSchemaIsolationIntegrationTest extends ContainerTestBase {
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final TableIdentifier TABLE = TableIdentifier.of("data", "events");
    private static final ConnectorSession READER = new ConnectorSession() {
        @Override public String getUser() { return "reader"; }
        @Override public Optional<String> getCatalog() { return Optional.empty(); }
        @Override public Optional<String> getSchema() { return Optional.of("data"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
        @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    };

    @TempDir
    Path warehouse;

    @Test
    void postgresConnectorUsesTheExactSpellingWithoutListingTheNamespace() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            selected.createNamespace(Namespace.of("data"));
            selected.createTable(TableIdentifier.of("data", "CamelEvents"), TABLE_SCHEMA);
            var connector = new IcebergConnectorFactory().create(schema.name(), jdbcProperties(schema, "V1"), null);
            try {
                var metadata = connector.getMetadata(READER);
                assertThat(metadata.supportsExactTableNameLookup()).isTrue();
                schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
                long listings = schema.namespaceListings();
                long lookups = schema.catalogPointerLookups();

                assertThat(metadata.getTableHandle(READER, new SchemaTableName("data", "CamelEvents")))
                        .isInstanceOf(IcebergTableHandle.class);
                assertThat(schema.catalogPointerLookups() - lookups).isEqualTo(1);
                assertThat(metadata.getTableHandle(READER, new SchemaTableName("data", "camelevents"))).isNull();
                assertThat(schema.namespaceListings()).isEqualTo(listings);

                assertThat(metadata.listTables(READER, Optional.of("data")))
                        .containsExactly(new SchemaTableName("data", "CamelEvents"));
                assertThat(schema.namespaceListings()).isGreaterThan(listings);
            } finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void freshJdbcTableResolutionChecksCurrentMetadataPointerOnce() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var tables = new IcebergCatalogTables(selected);
            var first = (IcebergTableHandle) tables.getTableHandle(new SchemaTableName("data", "events"));
            try (var writer = catalog(schema, "V1")) {
                writer.loadTable(TABLE).updateSchema().addColumn("new_field", Types.LongType.get()).commit();
            }
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            long before = schema.catalogPointerLookups();

            assertThat(tables.getTableReadMetadata(first).metadata().columns()).contains("new_field");

            assertThat(schema.catalogPointerLookups() - before).isEqualTo(1);
        }
    }

    @Test
    void anotherInitializedSchemaDoesNotPreventCreatingCatalogTables() throws Exception {
        try (var first = DatabaseSchema.create(); var second = DatabaseSchema.create();
                var firstCatalog = catalog(first, "V1")) {
            createTable(firstCatalog);
            try (var secondCatalog = catalog(second, "V1")) {
                createTable(secondCatalog);
                assertThat(secondCatalog.loadTable(TABLE).schema().columns()).hasSize(1);
                assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                assertThat(firstCatalog.loadTable(TABLE).location())
                        .isNotEqualTo(secondCatalog.loadTable(TABLE).location());
            }
        }
    }

    @Test
    void anotherV1SchemaDoesNotPreventUpgradingTheCurrentV0Schema() throws Exception {
        try (var first = DatabaseSchema.create(); var second = DatabaseSchema.create();
                var secondV0 = catalog(second, "V0")) {
            createTable(secondV0);
            assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isFalse();
            try (var firstV1 = catalog(first, "V1")) {
                createTable(firstV1);
                try (var secondV1 = catalog(second, "V1")) {
                    assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                    assertThat(secondV1.loadTable(TABLE).schema().columns()).hasSize(1);
                }
            }
        }
    }

    @Test
    void jdbcPatternWildcardsDoNotMatchUnrelatedTablesInTheSameSchema() throws Exception {
        try (var schema = DatabaseSchema.create()) {
            schema.execute("CREATE TABLE icebergXtables (icebergXtype VARCHAR(5))");
            schema.execute("CREATE TABLE icebergXnamespaceXproperties (id BIGINT)");
            try (var selected = catalog(schema, "V1")) {
                createTable(selected);
                assertThat(selected.loadTable(TABLE).schema().columns()).hasSize(1);
                assertThat(schema.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
            }
        }
    }

    @Test
    void jdbcPatternWildcardsDoNotMatchAnUnrelatedColumnDuringUpgrade() throws Exception {
        try (var schema = DatabaseSchema.create()) {
            try (var initial = catalog(schema, "V0")) {
                createTable(initial);
            }
            schema.execute("ALTER TABLE iceberg_tables ADD COLUMN icebergXtype VARCHAR(5)");
            try (var upgraded = catalog(schema, "V1")) {
                assertThat(schema.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                assertThat(upgraded.loadTable(TABLE).schema().columns()).hasSize(1);
            }
        }
    }

    @Test
    void crossNamespaceRenamePreservesTableUuidAndTheExactMetadataPointer() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var before = ((org.apache.iceberg.BaseTable) selected.loadTable(TABLE)).operations().current();
            selected.createNamespace(Namespace.of("renamed_namespace"));
            var target = TableIdentifier.of("renamed_namespace", "renamed_table");
            selected.renameTable(TABLE, target);
            var after = ((org.apache.iceberg.BaseTable) selected.loadTable(target)).operations().current();
            assertThat(selected.tableExists(TABLE)).isFalse();
            assertThat(after.uuid()).isEqualTo(before.uuid());
            assertThat(after.metadataFileLocation()).isEqualTo(before.metadataFileLocation());
        }
    }

    private JdbcCatalog catalog(DatabaseSchema schema, String version) {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return (JdbcCatalog) CatalogUtil.buildIcebergCatalog(schema.name(), jdbcProperties(schema, version), configuration);
    }

    private Map<String, String> jdbcProperties(DatabaseSchema schema, String version) {
        var postgres = IcebergJdbcTestDatabase.instance();
        return Map.of("type", "jdbc", "uri", schema.uri(),
                "warehouse", warehouse.resolve(schema.name()).toString(),
                "jdbc.user", postgres.getUsername(), "jdbc.password", postgres.getPassword(),
                "jdbc.schema-version", version);
    }

    private static void createTable(JdbcCatalog catalog) {
        catalog.createNamespace(Namespace.of("data"));
        catalog.createTable(TABLE, TABLE_SCHEMA);
    }

    private record DatabaseSchema(String name, String uri, Connection connection) implements AutoCloseable {
        static DatabaseSchema create() throws SQLException {
            var postgres = IcebergJdbcTestDatabase.instance();
            String name = "sdk_schema_" + UUID.randomUUID().toString().replace("-", "");
            String baseUri = postgres.getJdbcUrl();
            String uri = baseUri + (baseUri.contains("?") ? "&" : "?") + "currentSchema=" + name;
            var connection = DriverManager.getConnection(uri, postgres.getUsername(), postgres.getPassword());
            try {
                try (var statement = connection.createStatement()) {
                    statement.execute("CREATE SCHEMA " + name);
                }
                return new DatabaseSchema(name, uri, connection);
            } catch (SQLException failure) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        void execute(String sql) throws SQLException {
            try (var statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }

        boolean hasColumn(String table, String column) throws SQLException {
            try (var statement = connection.prepareStatement("SELECT COUNT(*) FROM information_schema.columns"
                    + " WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
                statement.setString(1, name);
                statement.setString(2, table);
                statement.setString(3, column);
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1) == 1;
                }
            }
        }

        long catalogPointerLookups() throws SQLException {
            try (var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                            WHERE query LIKE 'SELECT * FROM iceberg_tables WHERE catalog_name = %'
                              AND query LIKE '%AND table_name = %'
                            """)) {
                rows.next();
                return rows.getLong(1);
            }
        }

        long namespaceListings() throws SQLException {
            try (var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                            WHERE query LIKE 'SELECT * FROM iceberg_tables WHERE catalog_name = %'
                              AND query LIKE '%AND table_namespace = %'
                              AND query NOT LIKE '%AND table_name = %'
                            """)) {
                rows.next();
                return rows.getLong(1);
            }
        }

        @Override
        public void close() throws SQLException {
            try (connection) {
                execute("DROP SCHEMA " + name + " CASCADE");
            }
        }
    }
}
