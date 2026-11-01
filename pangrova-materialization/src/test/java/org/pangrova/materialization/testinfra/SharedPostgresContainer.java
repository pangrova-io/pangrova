package org.pangrova.materialization.testinfra;

import org.flywaydb.core.Flyway;
import org.pangrova.security.store.SecuritySchema;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

/**
 * Shared PostgreSQL container for materialization integration tests.
 * Uses singleton pattern — all tests share one container to avoid startup overhead.
 *
 * <p>The kernels tables are fully qualified by the materialization baseline, so the shared
 * {@code materialization} schema is partitioned by random tenant while the auxiliary tables
 * (anchors, journal, lease) live in per-test isolated schemas.</p>
 */
public final class SharedPostgresContainer {

    private static final String INDEXING_LOCATION = "db/indexing/migration";
    private static final String INDEXING_HISTORY_TABLE = "flyway_schema_history_indexing";
    private static final String QUERY_LOCATION = "db/query/migration";
    private static final String QUERY_HISTORY_TABLE = "flyway_schema_history_query";
    private static final String MATERIALIZATION_LOCATION = "db/materialization/migration";
    private static final String MATERIALIZATION_HISTORY_TABLE = "flyway_schema_history_materialization";

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
     * Creates an isolated schema and applies the owner baselines carried on the materialization
     * test classpath in production dependency order: the bundled security schema first (query
     * baseline references {@code storage_profile_revisions}), then indexing ({@code
     * index_commit_log}, read by the query baseline's write-commit-journal delete guard), query
     * ({@code execution_anchor}, {@code write_commit_journal}) and the materialization baseline
     * ({@code materialization} schema). The query baseline precedes the materialization baseline
     * because the materialization baseline installs its native-commit capture trigger on
     * {@code write_commit_journal}.
     *
     * <p>The coordinator-owned {@code distributed_lease} table is created inline because
     * pangrova-coordinator depends on pangrova-materialization in main scope, so a test-scope
     * back-dependency would cycle the reactor. Its shape mirrors the coordinator baseline
     * exactly; the production DDL stays with its owner.</p>
     */
    public static DataSource migratedDataSource(String prefix) throws SQLException {
        DataSource dataSource = isolatedDataSource(prefix);
        SecuritySchema.initialize(dataSource);
        migrate(dataSource, INDEXING_LOCATION, INDEXING_HISTORY_TABLE);
        migrate(dataSource, QUERY_LOCATION, QUERY_HISTORY_TABLE);
        createDistributedLease(dataSource);
        migrate(dataSource, MATERIALIZATION_LOCATION, MATERIALIZATION_HISTORY_TABLE);
        return dataSource;
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    /**
     * Recreates the coordinator-owner {@code distributed_lease} table (see
     * {@link #migratedDataSource(String)} for why it is not applied from the coordinator
     * baseline). The materialization baseline's {@code validate_attempt()} function resolves
     * {@code distributed_lease%ROWTYPE} at creation time, so the table must exist first.
     */
    private static void createDistributedLease(DataSource dataSource) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE distributed_lease ("
                    + "lease_key character varying(255) NOT NULL, "
                    + "holder_id character varying(255) NOT NULL, "
                    + "epoch bigint DEFAULT 0 NOT NULL, "
                    + "acquired_at timestamp(6) with time zone NOT NULL, "
                    + "expires_at timestamp(6) with time zone NOT NULL, "
                    + "version bigint DEFAULT 0 NOT NULL)");
            statement.execute("ALTER TABLE ONLY distributed_lease "
                    + "ADD CONSTRAINT distributed_lease_pkey PRIMARY KEY (lease_key)");
        }
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
                .withDatabaseName("pangrova_materialization_test")
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
