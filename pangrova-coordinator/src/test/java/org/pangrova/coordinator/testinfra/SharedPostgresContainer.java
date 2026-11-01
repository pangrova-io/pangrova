package org.pangrova.coordinator.testinfra;

import org.pangrova.metadata.store.jdbc.SchemaMigrationRunner;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;

/**
 * Shared PostgreSQL container for coordinator integration tests.
 *
 * <p>Uses a singleton container pattern: all tests share a single PostgreSQL
 * instance to avoid the overhead of starting a new container per test class.
 * Each test should use its own schema or table prefix to avoid interference.</p>
 */
public final class SharedPostgresContainer {

    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private SharedPostgresContainer() {}

    public static PostgreSQLContainer<?> instance() {
        return POSTGRES;
    }

    public static DataSource dataSource() {
        return dataSource(null);
    }

    /** Creates a private PostgreSQL schema for one test without sharing rows. */
    public static DataSource isolatedDataSource(String prefix) throws SQLException {
        String normalized = prefix.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]", "_");
        if (normalized.length() > 24) {
            normalized = normalized.substring(0, 24);
        }
        String schema = normalized + "_"
                + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        return dataSource(schema);
    }

    /** Creates an isolated schema and applies the per-owner production baselines
     *  (bundled security schema first, then all owners in dependency order). */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource dataSource = isolatedDataSource(prefix);
        SchemaMigrationRunner.migrateAll(dataSource);
        return dataSource;
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

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_test")
                .withUsername("pangrova")
                .withPassword("pangrova");
        container.start();
        return container;
    }
}
