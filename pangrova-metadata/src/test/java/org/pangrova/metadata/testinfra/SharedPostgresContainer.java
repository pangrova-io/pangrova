package org.pangrova.metadata.testinfra;

import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Shared PostgreSQL container for metadata integration tests.
 * Uses singleton pattern — all tests share one container to avoid startup overhead.
 */
public final class SharedPostgresContainer {

    private static final PostgreSQLContainer<?> POSTGRES;
    private static final boolean AVAILABLE;

    static {
        PostgreSQLContainer<?> container = null;
        boolean available = false;
        try {
            container = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_metadata_test")
                    .withUsername("pangrova")
                    .withPassword("pangrova");
            container.start();
            available = true;
        } catch (Exception e) {
            // Docker not available — tests will be skipped via assumeTrue(isAvailable())
        }
        POSTGRES = container;
        AVAILABLE = available;
    }

    private SharedPostgresContainer() {}

    public static boolean isAvailable() {
        return AVAILABLE;
    }

    public static DataSource dataSource() {
        return new EmbeddedDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static final class EmbeddedDataSource implements DataSource {
        private final String url;
        private final String username;
        private final String password;

        EmbeddedDataSource(String url, String username, String password) {
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
