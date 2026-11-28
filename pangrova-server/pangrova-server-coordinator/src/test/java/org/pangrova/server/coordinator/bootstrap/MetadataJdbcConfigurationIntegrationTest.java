package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.jdbc.datasource.PooledDataSourceFactory;
import org.pangrova.persistence.jdbc.migration.SchemaMigrationRunner;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real metadata pool startup through the process configuration; not a query/JNI E2E. */
class MetadataJdbcConfigurationIntegrationTest extends ContainerTestBase {
    @Test
    void configuredCredentialsMigrateAndPersistThroughTheProductionPool() throws Exception {
        var source = (PGSimpleDataSource) SharedPostgresContainer.isolatedDataSource("process_credentials");
        var jdbc = configured(source.getURL(), source.getUser(), source.getPassword());
        assertThat(jdbc.jdbcUrl()).doesNotContain("password=", "user=");
        try (var pool = PooledDataSourceFactory.create(jdbc.jdbcUrl(), jdbc.username(), jdbc.password())) {
            SchemaMigrationRunner.migrate(pool);
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                try (var identity = statement.executeQuery("SELECT CURRENT_USER")) {
                    assertThat(identity.next()).isTrue();
                    assertThat(identity.getString(1)).isEqualTo(source.getUser());
                }
                try (var migration = statement.executeQuery("SELECT to_regclass('flyway_schema_history')")) {
                    assertThat(migration.next()).isTrue();
                    assertThat(migration.getString(1)).isNotNull();
                }
                statement.execute("CREATE TABLE process_credentials_receipt (id BIGINT PRIMARY KEY, value TEXT)");
                statement.execute("INSERT INTO process_credentials_receipt VALUES (1, 'persisted-through-pool')");
            }
        }
        try (var pool = PooledDataSourceFactory.create(jdbc.jdbcUrl(), jdbc.username(), jdbc.password());
             var connection = pool.getConnection(); var statement = connection.createStatement();
             var receipt = statement.executeQuery("SELECT id, value FROM process_credentials_receipt")) {
            assertThat(receipt.next()).isTrue();
            assertThat(receipt.getLong(1)).isEqualTo(1L);
            assertThat(receipt.getString(2)).isEqualTo("persisted-through-pool");
            assertThat(receipt.next()).isFalse();
        }
    }

    @Test
    void actualDatabaseRejectsWrongConfiguredPasswordInsteadOfStartingAnAnonymousPool() {
        var postgres = SharedPostgresContainer.instance();
        var jdbc = configured(postgres.getJdbcUrl(), postgres.getUsername(), "deliberately-wrong-password");
        assertThatThrownBy(() -> {
            try (var pool = PooledDataSourceFactory.create(jdbc.jdbcUrl(), jdbc.username(), jdbc.password());
                 var ignored = pool.getConnection()) {
                throw new AssertionError("An incorrect PostgreSQL password must not acquire a connection");
            }
        }).isInstanceOf(com.zaxxer.hikari.pool.HikariPool.PoolInitializationException.class)
                .hasMessageContaining("password authentication failed");
    }

    private static CoordinatorBootstrapConfig.MetadataJdbcConfig configured(
            String url, String username, String password) {
        var configuration = PangrovaConfiguration.fromYaml(Map.of(
                "metadata.jdbc-url", url,
                "metadata.jdbc-username", username,
                "metadata.jdbc-password", password));
        return PangrovaCoordinatorMain.buildBootstrapConfig(configuration).metadataJdbcConfig();
    }
}
