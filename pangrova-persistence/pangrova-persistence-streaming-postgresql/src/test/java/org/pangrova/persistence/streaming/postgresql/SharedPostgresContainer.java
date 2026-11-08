package org.pangrova.persistence.streaming.postgresql;

import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

/**
 * Shared PostgreSQL container for streaming store integration tests.
 * Uses a singleton container — all tests share one container to avoid startup overhead.
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

    /** Creates an isolated schema and applies the complete production migration chain. */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource source = isolatedDataSource(prefix);
        Flyway.configure()
                .dataSource(source)
                .locations("classpath:db/migration")
                .load()
                .migrate();
        return source;
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_streaming_test")
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
