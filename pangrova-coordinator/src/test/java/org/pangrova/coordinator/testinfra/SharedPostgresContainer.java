package org.pangrova.coordinator.testinfra;

import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Shared PostgreSQL container for coordinator integration tests.
 *
 * <p>Uses a singleton container pattern: all tests share a single PostgreSQL
 * instance to avoid the overhead of starting a new container per test class.
 * Each test should use its own schema or table prefix to avoid interference.</p>
 *
 * <p>When Docker is not available (e.g., local development without Docker Desktop),
 * the container fails gracefully. Tests should call {@link #isAvailable()} or use
 * {@code Assumptions.assumeTrue(SharedPostgresContainer.isAvailable())} to skip.</p>
 */
public final class SharedPostgresContainer {

    private static final PostgreSQLContainer<?> POSTGRES;
    private static final boolean AVAILABLE;

    static {
        PostgreSQLContainer<?> container = null;
        boolean available = false;
        try {
            container = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_test")
                    .withUsername("pangrova")
                    .withPassword("pangrova");
            container.start();
            available = true;
        } catch (Exception e) {
            // Docker not available — tests will be skipped via isAvailable()
        }
        POSTGRES = container;
        AVAILABLE = available;
    }

    private SharedPostgresContainer() {}

    /**
     * Returns true if the PostgreSQL container started successfully (Docker is available).
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    public static PostgreSQLContainer<?> instance() {
        return POSTGRES;
    }

    public static DataSource dataSource() {
        return new SimplePostgresDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /**
     * Creates the orphaned_queries table required by {@code OrphanQueryReclaimer}.
     */
    public static void createOrphanedQueriesTable(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS orphaned_queries (
                        query_id VARCHAR(64) NOT NULL PRIMARY KEY,
                        original_coordinator VARCHAR(128) NOT NULL,
                        sql_text TEXT,
                        session_json TEXT,
                        released_at BIGINT NOT NULL,
                        claimed_by VARCHAR(128) DEFAULT NULL,
                        claimed_at BIGINT DEFAULT NULL
                    )
                    """);
        }
    }

    /**
     * Creates the worker_registry table required by {@code JdbcWorkerRegistry}.
     */
    public static void createWorkerRegistryTable(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS worker_registry (
                        worker_id VARCHAR(255) PRIMARY KEY,
                        host VARCHAR(255) NOT NULL,
                        http_port INT NOT NULL,
                        transport_port INT NOT NULL,
                        protocol_version INT NOT NULL,
                        capabilities TEXT,
                        rack_id VARCHAR(255),
                        data_locality TEXT,
                        memory_pressure VARCHAR(50),
                        cpu_utilization DOUBLE PRECISION,
                        running_task_count INT,
                        available_memory_bytes BIGINT,
                        last_heartbeat_ms BIGINT,
                        registered_at_ms BIGINT
                    )
                    """);
        }
    }

    /**
     * Truncates the given table. Used in @BeforeEach to reset state between tests.
     */
    public static void truncateTable(DataSource dataSource, String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + tableName);
        }
    }

    /**
     * Minimal DataSource backed by PostgreSQL Testcontainer.
     * No connection pooling — sufficient for integration tests.
     */
    private static final class SimplePostgresDataSource implements DataSource {
        private final String url;
        private final String username;
        private final String password;

        SimplePostgresDataSource(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public Connection getConnection(String u, String p) throws SQLException {
            return DriverManager.getConnection(url, u, p);
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) {}
        @Override public void setLoginTimeout(int seconds) {}
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() { return Logger.getLogger("SharedPostgresContainer"); }
        @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
