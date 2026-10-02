package org.pangrova.security.testinfra;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.UUID;

/** One JVM-level PostgreSQL instance with an isolated schema per security test. */
public final class SharedSecurityPostgres {
    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private SharedSecurityPostgres() {}

    /** Returns a datasource whose objects cannot collide with another test's rows. */
    public static DataSource isolatedDataSource() throws SQLException {
        String schema = "security_" + UUID.randomUUID().toString().replace("-", "");
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        source.setCurrentSchema(schema);
        return source;
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        container.start();
        return container;
    }
}
