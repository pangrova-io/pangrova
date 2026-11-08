package org.pangrova.persistence.streaming.postgresql;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.UUID;

/** One real PostgreSQL process per test JVM, with a private schema per CDC test. */
public final class CdcCheckpointDatabase {
    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private CdcCheckpointDatabase() {}

    public static DataSource isolatedDataSource() throws SQLException {
        var source = new PGSimpleDataSource();
        source.setUrl(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        String schema = "cdc_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        source.setCurrentSchema(schema);
        return source;
    }

    private static PostgreSQLContainer<?> startPostgres() {
        var container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
                .withUsername("pangrova").withPassword("pangrova_test");
        container.start();
        return container;
    }
}
