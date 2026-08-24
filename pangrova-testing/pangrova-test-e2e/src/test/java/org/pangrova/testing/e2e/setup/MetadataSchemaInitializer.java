package org.pangrova.testing.e2e.setup;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Creates the PostgreSQL schema tables required by JdbcMetadataStore.
 * Used during E2E test setup to initialize the metadata store backend.
 */
public final class MetadataSchemaInitializer {

    private MetadataSchemaInitializer() {}

    /**
     * Creates a minimal DataSource from JDBC connection parameters.
     */
    public static DataSource createDataSource(String url, String user, String password) {
        return new EmbeddedDataSource(url, user, password);
    }

    /**
     * Creates all metadata store tables in PostgreSQL.
     * Idempotent: uses IF NOT EXISTS to avoid failures on re-run.
     */
    public static void createSchema(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(CATALOG_DEFINITIONS_DDL);
            stmt.execute(SCHEMA_METADATA_DDL);
            stmt.execute(TABLE_METADATA_DDL);
            stmt.execute(SYNC_HISTORY_DDL);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_catalog_time ON sync_history (catalog_name, sync_time)");
            // Index metadata tables (V7 + V8)
            stmt.execute(INDEX_DESCRIPTORS_DDL);
            stmt.execute(INDEX_FILE_MAPPINGS_DDL);
            stmt.execute(DISTRIBUTED_LEASE_DDL);
            stmt.execute(INDEX_COMMIT_LOG_DDL);
            stmt.execute(INDEX_BUILD_CLAIMS_DDL);
            stmt.execute(INDEX_CACHE_EVENTS_DDL);
            stmt.execute(INDEX_EVAL_BREAKER_STATE_DDL);
        }
    }

    private static final String CATALOG_DEFINITIONS_DDL = """
            CREATE TABLE IF NOT EXISTS catalog_definitions (
                name VARCHAR(255) PRIMARY KEY,
                type VARCHAR(50) NOT NULL,
                provider VARCHAR(50) NOT NULL,
                state VARCHAR(20) NOT NULL DEFAULT 'CREATING',
                properties TEXT NOT NULL,
                comment TEXT,
                allowed_tenants TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""";

    private static final String SCHEMA_METADATA_DDL = """
            CREATE TABLE IF NOT EXISTS schema_metadata (
                catalog_name VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                properties TEXT,
                synced_at TIMESTAMP NOT NULL,
                PRIMARY KEY (catalog_name, schema_name),
                FOREIGN KEY (catalog_name) REFERENCES catalog_definitions(name) ON DELETE CASCADE
            )""";

    private static final String TABLE_METADATA_DDL = """
            CREATE TABLE IF NOT EXISTS table_metadata (
                catalog_name VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                columns TEXT NOT NULL,
                partition_spec TEXT,
                sort_order TEXT,
                properties TEXT,
                table_created_at TIMESTAMP NULL,
                table_updated_at TIMESTAMP NULL,
                synced_at TIMESTAMP NOT NULL,
                PRIMARY KEY (catalog_name, schema_name, table_name),
                FOREIGN KEY (catalog_name, schema_name)
                    REFERENCES schema_metadata(catalog_name, schema_name) ON DELETE CASCADE
            )""";

    private static final String SYNC_HISTORY_DDL = """
            CREATE TABLE IF NOT EXISTS sync_history (
                id BIGSERIAL PRIMARY KEY,
                catalog_name VARCHAR(255) NOT NULL,
                sync_type VARCHAR(20) NOT NULL,
                sync_time TIMESTAMP NOT NULL,
                changes_applied INT NOT NULL DEFAULT 0,
                errors INT NOT NULL DEFAULT 0,
                error_messages TEXT,
                duration_ms BIGINT NOT NULL
            )""";

    // ── Index metadata tables (V7) ──

    private static final String INDEX_DESCRIPTORS_DDL = """
            CREATE TABLE IF NOT EXISTS index_descriptors (
                index_id VARCHAR(255) PRIMARY KEY,
                index_name VARCHAR(191) NOT NULL,
                catalog_name VARCHAR(191) NOT NULL,
                schema_name VARCHAR(191) NOT NULL,
                table_name VARCHAR(191) NOT NULL,
                column_names TEXT NOT NULL,
                index_type VARCHAR(50) NOT NULL,
                properties TEXT NOT NULL,
                state VARCHAR(20) NOT NULL,
                stale_reason VARCHAR(1024),
                generation INT NOT NULL DEFAULT 0,
                build_snapshot_id VARCHAR(255),
                last_indexed_snapshot_id VARCHAR(255),
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                build_owner VARCHAR(255),
                build_heartbeat_at TIMESTAMP,
                CONSTRAINT uq_index_name_per_table
                    UNIQUE (catalog_name, schema_name, table_name, index_name)
            )""";

    private static final String INDEX_FILE_MAPPINGS_DDL = """
            CREATE TABLE IF NOT EXISTS index_file_mappings (
                id BIGSERIAL PRIMARY KEY,
                index_id VARCHAR(255) NOT NULL,
                data_file_path VARCHAR(768) NOT NULL,
                index_file_path VARCHAR(768) NOT NULL,
                row_count BIGINT NOT NULL DEFAULT 0,
                index_size_bytes BIGINT NOT NULL DEFAULT 0,
                checksum VARCHAR(64),
                generation INT NOT NULL DEFAULT 0,
                snapshot_token BIGINT DEFAULT 0,
                sequence_num BIGINT NOT NULL DEFAULT 0,
                last_verified_at BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL,
                CONSTRAINT fk_index_file_mapping_index
                    FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE
            )""";

    // ── Distributed coordination tables (V8) ──

    private static final String INDEX_COMMIT_LOG_DDL = """
            CREATE TABLE IF NOT EXISTS index_commit_log (
                event_id BIGSERIAL PRIMARY KEY,
                catalog_name VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                snapshot_id VARCHAR(255),
                added_files TEXT,
                removed_files TEXT,
                event_type VARCHAR(50) NOT NULL DEFAULT 'APPEND',
                processed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL
            )""";

    private static final String INDEX_BUILD_CLAIMS_DDL = """
            CREATE TABLE IF NOT EXISTS index_build_claims (
                index_id VARCHAR(255) NOT NULL,
                generation INT NOT NULL,
                owner_id VARCHAR(255) NOT NULL,
                claimed_at TIMESTAMP NOT NULL,
                heartbeat_at TIMESTAMP NOT NULL,
                PRIMARY KEY (index_id, generation)
            )""";

    private static final String INDEX_CACHE_EVENTS_DDL = """
            CREATE TABLE IF NOT EXISTS index_cache_events (
                event_id BIGSERIAL PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                index_id VARCHAR(255),
                catalog_name VARCHAR(255),
                schema_name VARCHAR(255),
                table_name VARCHAR(255),
                created_at TIMESTAMP NOT NULL
            )""";

    // ── Unified distributed lease table ──

    private static final String DISTRIBUTED_LEASE_DDL = """
            CREATE TABLE IF NOT EXISTS distributed_lease (
                lease_key   VARCHAR(255) PRIMARY KEY,
                holder_id   VARCHAR(255) NOT NULL,
                epoch       BIGINT       NOT NULL DEFAULT 0,
                acquired_at TIMESTAMP(3) NOT NULL,
                expires_at  TIMESTAMP(3) NOT NULL,
                version     BIGINT       NOT NULL DEFAULT 0
            )""";

    private static final String INDEX_EVAL_BREAKER_STATE_DDL = """
            CREATE TABLE IF NOT EXISTS index_eval_breaker_state (
                index_id VARCHAR(255) PRIMARY KEY,
                failure_count INT NOT NULL DEFAULT 0,
                last_failure_at TIMESTAMP,
                last_success_at TIMESTAMP,
                state VARCHAR(20) NOT NULL DEFAULT 'CLOSED'
            )""";

    // ── State Store DDLs (coordination, governance, streaming, tenant) ──

    private static final String QUERY_STATE_JOURNAL_DDL = """
            CREATE TABLE IF NOT EXISTS query_state_journal (
                id BIGSERIAL PRIMARY KEY,
                query_id VARCHAR(64) NOT NULL,
                sql_text TEXT,
                session_json TEXT,
                state VARCHAR(32) NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                coordinator_id VARCHAR(128),
                epoch BIGINT NOT NULL DEFAULT 0
            )""";

    private static final String TASK_STATE_JOURNAL_DDL = """
            CREATE TABLE IF NOT EXISTS task_state_journal (
                id BIGSERIAL PRIMARY KEY,
                query_id VARCHAR(64) NOT NULL,
                task_id VARCHAR(64) NOT NULL,
                worker_id VARCHAR(64),
                state VARCHAR(32) NOT NULL,
                stage_id INT NOT NULL,
                report_sequence BIGINT NOT NULL DEFAULT 0,
                timestamp_ms BIGINT NOT NULL,
                coordinator_id VARCHAR(128),
                epoch BIGINT NOT NULL DEFAULT 0
            )""";

    private static final String WORKER_REGISTRY_DDL = """
            CREATE TABLE IF NOT EXISTS worker_registry (
                worker_id VARCHAR(128) NOT NULL PRIMARY KEY,
                host VARCHAR(256) NOT NULL,
                http_port INT NOT NULL,
                transport_port INT NOT NULL,
                protocol_version INT NOT NULL DEFAULT 1,
                capabilities TEXT,
                rack_id VARCHAR(128) DEFAULT NULL,
                data_locality TEXT,
                memory_pressure VARCHAR(32) NOT NULL DEFAULT 'NONE',
                cpu_utilization DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                running_task_count INT NOT NULL DEFAULT 0,
                available_memory_bytes BIGINT NOT NULL DEFAULT 0,
                last_heartbeat_ms BIGINT NOT NULL,
                registered_at_ms BIGINT NOT NULL
            )""";

    private static final String COORDINATOR_REGISTRY_DDL = """
            CREATE TABLE IF NOT EXISTS coordinator_registry (
                coordinator_id VARCHAR(128) NOT NULL PRIMARY KEY,
                host VARCHAR(256) NOT NULL,
                http_port INT NOT NULL,
                role VARCHAR(32) NOT NULL DEFAULT 'STANDBY',
                epoch BIGINT NOT NULL DEFAULT 0,
                last_heartbeat_ms BIGINT NOT NULL,
                registered_at_ms BIGINT NOT NULL
            )""";

    private static final String ORPHANED_QUERIES_DDL = """
            CREATE TABLE IF NOT EXISTS orphaned_queries (
                query_id VARCHAR(64) NOT NULL PRIMARY KEY,
                original_coordinator VARCHAR(128) NOT NULL,
                sql_text TEXT,
                session_json TEXT,
                released_at BIGINT NOT NULL,
                claimed_by VARCHAR(128) DEFAULT NULL,
                claimed_at BIGINT DEFAULT NULL
            )""";

    private static final String TENANTS_DDL = """
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id VARCHAR(255) NOT NULL PRIMARY KEY,
                name VARCHAR(512) NOT NULL,
                status VARCHAR(32) NOT NULL,
                max_memory_bytes BIGINT NOT NULL DEFAULT 0,
                max_concurrent_queries INT NOT NULL DEFAULT 0,
                max_queued_queries INT NOT NULL DEFAULT 0,
                max_cpu_time_per_query_ms BIGINT NOT NULL DEFAULT 0,
                max_scan_bytes_per_query BIGINT NOT NULL DEFAULT 0,
                max_daily_query_count BIGINT NOT NULL DEFAULT 0,
                max_daily_scan_bytes BIGINT NOT NULL DEFAULT 0,
                max_storage_bytes BIGINT NOT NULL DEFAULT 0,
                allowed_catalogs TEXT,
                allowed_schemas TEXT,
                resource_pool VARCHAR(255),
                properties TEXT,
                created_at TIMESTAMP NOT NULL,
                created_by VARCHAR(255)
            )""";

    private static final String QUALITY_RULES_DDL = """
            CREATE TABLE IF NOT EXISTS quality_rules (
                rule_id BIGINT NOT NULL PRIMARY KEY,
                rule_name VARCHAR(512) NOT NULL,
                catalog_name VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                check_expression TEXT NOT NULL,
                severity VARCHAR(32) NOT NULL,
                message VARCHAR(1024),
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                schedule VARCHAR(128),
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""";

    private static final String CDC_REGISTRATIONS_DDL = """
            CREATE TABLE IF NOT EXISTS cdc_registrations (
                stream_id VARCHAR(255) NOT NULL PRIMARY KEY,
                consumer_id VARCHAR(255) NOT NULL,
                tenant_id VARCHAR(255),
                state VARCHAR(32) NOT NULL,
                max_concurrent_writes INT NOT NULL DEFAULT 4,
                max_pending_bytes BIGINT NOT NULL DEFAULT 67108864,
                last_heartbeat TIMESTAMP,
                created_at TIMESTAMP NOT NULL
            )""";

    private static final String WATERMARK_POSITIONS_DDL = """
            CREATE TABLE IF NOT EXISTS watermark_positions (
                source_id VARCHAR(512) NOT NULL PRIMARY KEY,
                position TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""";

    private static final String INVALIDATION_LOG_DDL = """
            CREATE TABLE IF NOT EXISTS invalidation_log (
                id BIGSERIAL PRIMARY KEY,
                cache_key VARCHAR(1024) NOT NULL,
                invalidation_type VARCHAR(64) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""";

    /**
     * Creates all state store tables required for coordination, governance,
     * streaming, and tenant modules. Idempotent via IF NOT EXISTS.
     * Complements {@link #createSchema(DataSource)} which handles metadata tables.
     */
    public static void createStateStoreTables(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(QUERY_STATE_JOURNAL_DDL);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_qsj_query_state ON query_state_journal (query_id, state)");
            stmt.execute(TASK_STATE_JOURNAL_DDL);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_tsj_task_query ON task_state_journal (query_id, task_id)");
            stmt.execute(WORKER_REGISTRY_DDL);
            stmt.execute(COORDINATOR_REGISTRY_DDL);
            stmt.execute(ORPHANED_QUERIES_DDL);
            stmt.execute(TENANTS_DDL);
            stmt.execute(QUALITY_RULES_DDL);
            stmt.execute(CDC_REGISTRATIONS_DDL);
            stmt.execute(WATERMARK_POSITIONS_DDL);
            stmt.execute(INVALIDATION_LOG_DDL);
        }
    }
    /**
     * Minimal DataSource backed by DriverManager. No connection pooling.
     */
    static final class EmbeddedDataSource implements DataSource {

        private final String url;
        private final String user;
        private final String password;

        EmbeddedDataSource(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, user, password);
        }

        @Override
        public Connection getConnection(String u, String p) throws SQLException {
            return DriverManager.getConnection(url, u, p);
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() { return Logger.getLogger("EmbeddedDataSource"); }
        @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
