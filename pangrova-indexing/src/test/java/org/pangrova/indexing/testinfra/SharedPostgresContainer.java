package org.pangrova.indexing.testinfra;

import org.flywaydb.core.Flyway;
import org.pangrova.security.store.SecuritySchema;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

/**
 * Shared PostgreSQL container for indexing integration tests.
 * Uses singleton pattern — all tests share one container to avoid startup overhead.
 */
public final class SharedPostgresContainer {

    private static final String AI_LOCATION = "db/ai/evaluation/migration";
    private static final String AI_HISTORY_TABLE = "flyway_schema_history_ai";
    private static final String INDEXING_LOCATION = "db/indexing/migration";
    private static final String INDEXING_HISTORY_TABLE = "flyway_schema_history_indexing";
    private static final String QUERY_LOCATION = "db/query/migration";
    private static final String QUERY_HISTORY_TABLE = "flyway_schema_history_query";

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

    /**
     * Creates an isolated schema and applies the owner baselines carried on the indexing test
     * classpath in production dependency order: the bundled security schema first (query baseline
     * references {@code storage_profile_revisions}), then ai (tabular export and catalog export
     * tables), indexing and query ({@code execution_anchor}). Metadata-owner tables are
     * deliberately absent: real deferred table-control policy composition is verified in
     * pangrova-metadata, and indexing tests inject a permissive test double instead.
     */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource dataSource = isolatedDataSource(prefix);
        SecuritySchema.initialize(dataSource);
        migrate(dataSource, AI_LOCATION, AI_HISTORY_TABLE);
        migrate(dataSource, INDEXING_LOCATION, INDEXING_HISTORY_TABLE);
        migrate(dataSource, QUERY_LOCATION, QUERY_HISTORY_TABLE);
        return dataSource;
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static int migrate(DataSource dataSource, String location, String historyTable) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:" + location)
                .table(historyTable)
                // Owner baselines share one schema, so earlier owners' tables make the schema
                // non-empty; baseline at 0 instead of failing on the missing history table.
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        return flyway.migrate().migrationsExecuted;
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_indexing_test")
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
