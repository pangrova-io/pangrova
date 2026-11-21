package org.pangrova.testing.integration.e2e.setup;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.integration.e2e.SharedTestContainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies E2E setup runs the production migration chain, including new catalog fences. */
class MetadataSchemaInitializerTest extends ContainerTestBase {

    @Test
    void bothInitializationEntriesApplyProductionMigrationsIdempotently() throws Exception {
        String schema = "e2e_migration_" + UUID.randomUUID().toString().replace("-", "");
        var postgres = SharedTestContainers.POSTGRES;
        assertThat(postgres).isNotNull();
        DataSource admin = MetadataSchemaInitializer.createDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        try (Connection connection = admin.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        try {
            String jdbcUrl = postgres.getJdbcUrl()
                    + (postgres.getJdbcUrl().contains("?") ? "&" : "?")
                    + "currentSchema=" + schema;
            DataSource migrated = MetadataSchemaInitializer.createDataSource(
                    jdbcUrl, postgres.getUsername(), postgres.getPassword());

            MetadataSchemaInitializer.createStateStoreTables(migrated);
            long applied = successfulMigrationCount(migrated);
            assertThat(applied).isGreaterThan(0);
            assertThat(hasColumn(migrated, schema, "catalog_definitions", "incarnation_id")).isTrue();
            assertThat(hasColumn(migrated, schema, "catalog_definitions", "revision")).isTrue();
            assertThat(hasTable(migrated, schema, "catalog_descriptor_revisions")).isTrue();
            assertThat(hasTable(migrated, schema, "query_state_journal")).isTrue();
            assertThat(hasTable(migrated, schema, "worker_registry")).isTrue();

            MetadataSchemaInitializer.createSchema(migrated);
            MetadataSchemaInitializer.createStateStoreTables(migrated);
            assertThat(successfulMigrationCount(migrated)).isEqualTo(applied);
        } finally {
            try (Connection connection = admin.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        }
    }

    private static long successfulMigrationCount(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT count(*) FROM flyway_schema_history WHERE success")) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static boolean hasColumn(
            DataSource dataSource, String schema, String table, String column) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             var query = connection.prepareStatement(
                     "SELECT 1 FROM information_schema.columns "
                             + "WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
            query.setString(1, schema);
            query.setString(2, table);
            query.setString(3, column);
            try (ResultSet rows = query.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static boolean hasTable(DataSource dataSource, String schema, String table)
            throws SQLException {
        try (Connection connection = dataSource.getConnection();
             var query = connection.prepareStatement(
                     "SELECT 1 FROM information_schema.tables "
                             + "WHERE table_schema = ? AND table_name = ?")) {
            query.setString(1, schema);
            query.setString(2, table);
            try (ResultSet rows = query.executeQuery()) {
                return rows.next();
            }
        }
    }
}
