package org.pangrova.testing.integration.e2e.setup;

import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Initializes E2E PostgreSQL databases through the same versioned migrations
 * used by production. There is no test-only DDL path to drift from the catalog
 * and coordination store schemas.
 */
public final class MetadataSchemaInitializer {

    private MetadataSchemaInitializer() {}

    /** Creates a minimal DataSource from JDBC connection parameters. */
    public static DataSource createDataSource(String url, String user, String password) {
        return new EmbeddedDataSource(url, user, password);
    }

    /** Applies every pending production schema migration; safe to call again. */
    public static void createSchema(DataSource dataSource) throws SQLException {
        SchemaMigrationRunner.migrate(dataSource);
    }

    /**
     * Compatibility entry point for E2E callers that initialize only the state
     * stores. The production migration chain creates these and metadata tables
     * together, and Flyway skips already applied versions.
     */
    public static void createStateStoreTables(DataSource dataSource) throws SQLException {
        createSchema(dataSource);
    }

    /** Minimal DataSource backed by DriverManager. No connection pooling. */
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
