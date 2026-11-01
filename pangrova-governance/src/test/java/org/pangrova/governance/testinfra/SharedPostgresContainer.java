package org.pangrova.governance.testinfra;

import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared PostgreSQL container for governance integration tests. All tests share one container;
 * each test gets its own isolated schema plus the governance owner baseline.
 */
public final class SharedPostgresContainer {

    /** Classpath location of the governance-owner baseline. */
    public static final String GOVERNANCE_LOCATION = "db/governance/migration";

    /** History table used by the governance-owner baseline. */
    public static final String GOVERNANCE_HISTORY_TABLE = "flyway_schema_history_governance";

    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private SharedPostgresContainer() {}

    public static DataSource dataSource() {
        return dataSource(null);
    }

    /** Creates an isolated schema so concurrent test classes cannot share tables or rows. */
    public static DataSource isolatedDataSource(String prefix) throws SQLException {
        String normalized = prefix.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]", "_");
        if (normalized.length() > 24) {
            normalized = normalized.substring(0, 24);
        }
        String schema = normalized + "_"
                + UUID.randomUUID().toString().replace("-", "");
        try (var connection = dataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        return dataSource(schema);
    }

    /** Creates an isolated schema and applies the governance baseline to it. */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource dataSource = isolatedDataSource(prefix);
        migrate(dataSource);
        return dataSource;
    }

    /** Applies the governance-owner baseline to the given data source (idempotent per schema). */
    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:" + GOVERNANCE_LOCATION)
                .table(GOVERNANCE_HISTORY_TABLE)
                .baselineOnMigrate(true)
                .baselineVersion("0")
                // Missing-schema failures must surface (SQLState 3F000) instead of being
                // silently papered over by auto-creating the schema.
                .createSchemas(false)
                .load()
                .migrate();
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_governance_test")
                .withUsername("pangrova")
                .withPassword("pangrova");
        container.start();
        return container;
    }

    private static DataSource dataSource(String schema) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        if (schema != null) {
            dataSource.setCurrentSchema(schema);
        }
        return dataSource;
    }
}
