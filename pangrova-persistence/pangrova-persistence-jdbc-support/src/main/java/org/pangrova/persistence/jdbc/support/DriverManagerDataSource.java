package org.pangrova.persistence.jdbc.support;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal {@link DataSource} backed by {@link DriverManager} for bootstrap-time connections.
 * No connection pooling — production deployments should replace with a pooled DataSource.
 *
 * <p><strong>Connection creation:</strong> Every call to {@link #getConnection()} creates a new
 * physical JDBC connection via {@code DriverManager}. There is no pooling, no reuse, and no
 * automatic close tracking. Callers are responsible for closing each connection they obtain.
 *
 * <p><strong>Intended usage:</strong> Bootstrap, schema migration, and test scenarios only.
 * Production environments must replace this with a connection-pooled DataSource
 * (e.g. HikariCP) to avoid connection exhaustion and latency overhead.
 *
 * <p>Implements {@link AutoCloseable} so it can be used in try-with-resources blocks.
 * The {@link #close()} method performs best-effort leak detection by logging a warning
 * if any connections were opened during the lifetime of this data source.
 *
 * <p><b>Not suitable for production use</b> — every call creates a new physical
 * connection instead of borrowing from a pool.</p>
 */
public final class DriverManagerDataSource implements DataSource, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DriverManagerDataSource.class);

    private final String url;
    private final String user;
    private final String password;
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    public DriverManagerDataSource(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        connectionCount.incrementAndGet();
        LOG.debug("action=getConnection url={} user={}", url, user);
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public Connection getConnection(String u, String p) throws SQLException {
        connectionCount.incrementAndGet();
        LOG.debug("action=getConnection url={} user={}", url, u);
        return DriverManager.getConnection(url, u, p);
    }

    @Override
    public void close() {
        int count = connectionCount.get();
        if (count > 0) {
            LOG.warn("action=driver_manager_datasource_close openedConnections={}", count);
        }
    }

    @Override public PrintWriter getLogWriter() { return null; }
    @Override public void setLogWriter(PrintWriter out) { }
    @Override public void setLoginTimeout(int seconds) { }
    @Override public int getLoginTimeout() { return 0; }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("DriverManagerDataSource");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isAssignableFrom(getClass());
    }
}
