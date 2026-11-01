package org.pangrova.streaming.testinfra;

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

    /** Isolated schema with the streaming state, offset, and watermark tables. */
    public static DataSource isolatedStreamingDataSource() throws SQLException {
        DataSource source = isolatedDataSource();
        createStreamingSchema(source);
        return source;
    }

    public static void createStreamingSchema(DataSource source) throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_stream_state (
                        stream_name VARCHAR(255) NOT NULL PRIMARY KEY,
                        tenant_id VARCHAR(255) NOT NULL,
                        checkpoint_id VARCHAR(255) NOT NULL UNIQUE,
                        lifecycle_state VARCHAR(32) NOT NULL,
                        target_table VARCHAR(512) NOT NULL,
                        source_properties TEXT NOT NULL,
                        source_format VARCHAR(64) NOT NULL,
                        stream_properties TEXT,
                        creator_identity VARCHAR(255),
                        coordinator_id VARCHAR(255),
                        epoch BIGINT NOT NULL DEFAULT 0,
                        schema_version INT NOT NULL DEFAULT 0,
                        error_message TEXT,
                        records_consumed BIGINT NOT NULL DEFAULT 0,
                        records_written BIGINT NOT NULL DEFAULT 0,
                        records_dead_lettered BIGINT NOT NULL DEFAULT 0,
                        last_offset_commit_time TIMESTAMP,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL
                    )""");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_stream_offsets (
                        stream_name VARCHAR(255) NOT NULL,
                        topic VARCHAR(512) NOT NULL,
                        partition_id INT NOT NULL,
                        committed_offset BIGINT NOT NULL,
                        updated_at TIMESTAMP NOT NULL,
                        PRIMARY KEY (stream_name, topic, partition_id),
                        FOREIGN KEY (stream_name) REFERENCES ingestion_stream_state(stream_name)
                            ON DELETE CASCADE
                    )""");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS watermark_positions (
                        source_id VARCHAR(512) NOT NULL PRIMARY KEY,
                        position TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )""");
        }
    }

    private static PostgreSQLContainer<?> startPostgres() {
        var container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
                .withUsername("pangrova").withPassword("pangrova_test");
        container.start();
        return container;
    }
}
