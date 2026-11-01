package org.pangrova.metadata.testinfra;

import org.pangrova.metadata.store.jdbc.SchemaMigrationRunner;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

/**
 * Shared PostgreSQL container for metadata integration tests.
 * Uses singleton pattern — all tests share one container to avoid startup overhead.
 */
public final class SharedPostgresContainer {

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

    /** Creates an isolated schema and applies the per-owner production baselines present on
     *  the test classpath (bundled security schema first, then metadata and the other owners). */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource dataSource = isolatedDataSource(prefix);
        SchemaMigrationRunner.migrateAll(dataSource);
        return dataSource;
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
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
