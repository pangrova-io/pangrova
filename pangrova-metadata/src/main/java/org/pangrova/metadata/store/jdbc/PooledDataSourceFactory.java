package org.pangrova.metadata.store.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Creates a production-grade connection-pooled {@link DataSource} backed by HikariCP.
 *
 * <p>Replaces {@link EmbeddedDataSource} for production use. The pool provides
 * connection reuse, health checking, and bounded concurrency to prevent
 * connection exhaustion under load.</p>
 *
 * <p>Configuration defaults are tuned for a metadata store workload:
 * short-lived queries, moderate concurrency, PostgreSQL backend.</p>
 */
public final class PooledDataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PooledDataSourceFactory.class);

    private static final int DEFAULT_MAX_POOL_SIZE = 10;
    private static final int DEFAULT_MIN_IDLE = 2;
    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;
    private static final long DEFAULT_IDLE_TIMEOUT_MS = 600_000;
    private static final long DEFAULT_MAX_LIFETIME_MS = 1_800_000;

    private PooledDataSourceFactory() {
    }

    /**
     * Creates a HikariCP-backed DataSource with sensible defaults for metadata workloads.
     *
     * @param jdbcUrl  JDBC connection URL (e.g. "jdbc:postgresql://host:5432/pangrova")
     * @param username database username
     * @param password database password
     * @return a pooled, closeable DataSource
     */
    public static HikariDataSource create(String jdbcUrl, String username, String password) {
        return create(jdbcUrl, username, password, DEFAULT_MAX_POOL_SIZE);
    }

    /**
     * Creates a HikariCP-backed DataSource with explicit pool size.
     *
     * @param jdbcUrl     JDBC connection URL
     * @param username    database username
     * @param password    database password
     * @param maxPoolSize maximum number of connections in the pool
     * @return a pooled, closeable DataSource
     */
    public static HikariDataSource create(String jdbcUrl, String username, String password, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setPoolName("pangrova-metadata-pool");
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(Math.min(DEFAULT_MIN_IDLE, maxPoolSize));
        config.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT_MS);
        config.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_MS);
        config.setMaxLifetime(DEFAULT_MAX_LIFETIME_MS);
        config.setAutoCommit(true);

        HikariDataSource dataSource = new HikariDataSource(config);
        LOG.info("event=pooled_datasource_created jdbcUrl={} maxPoolSize={} minIdle={}",
                jdbcUrl, maxPoolSize, config.getMinimumIdle());
        return dataSource;
    }
}
