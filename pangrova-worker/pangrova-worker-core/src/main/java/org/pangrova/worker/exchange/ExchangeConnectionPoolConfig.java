package org.pangrova.worker.exchange;

/**
 * Configuration for exchange client connection pooling.
 *
 * @param maxConnectionsPerHost maximum number of connections per remote host
 * @param idleTimeoutSeconds    seconds before an idle connection is evicted
 * @param healthCheckIntervalSeconds interval between connection health checks
 * @param maxTotalConnections   maximum total connections across all hosts
 */
public record ExchangeConnectionPoolConfig(
        int maxConnectionsPerHost,
        int idleTimeoutSeconds,
        int healthCheckIntervalSeconds,
        int maxTotalConnections) {

    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 10;
    public static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 300;
    public static final int DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 30;
    public static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 200;

    public ExchangeConnectionPoolConfig {
        if (maxConnectionsPerHost <= 0) {
            throw new IllegalArgumentException(
                    "maxConnectionsPerHost must be positive: " + maxConnectionsPerHost);
        }
        if (idleTimeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "idleTimeoutSeconds must be positive: " + idleTimeoutSeconds);
        }
        if (healthCheckIntervalSeconds <= 0) {
            throw new IllegalArgumentException(
                    "healthCheckIntervalSeconds must be positive: " + healthCheckIntervalSeconds);
        }
        if (maxTotalConnections < maxConnectionsPerHost) {
            throw new IllegalArgumentException(
                    "maxTotalConnections must be >= maxConnectionsPerHost: "
                            + maxTotalConnections + " < " + maxConnectionsPerHost);
        }
    }

    /** Returns the default connection pool configuration. */
    public static ExchangeConnectionPoolConfig defaults() {
        return new ExchangeConnectionPoolConfig(
                DEFAULT_MAX_CONNECTIONS_PER_HOST,
                DEFAULT_IDLE_TIMEOUT_SECONDS,
                DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS,
                DEFAULT_MAX_TOTAL_CONNECTIONS);
    }
}
