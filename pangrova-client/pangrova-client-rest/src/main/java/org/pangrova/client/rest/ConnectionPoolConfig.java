package org.pangrova.client.rest;

import java.time.Duration;
import java.util.List;

/**
 * Immutable configuration for client connection pooling, health checks,
 * and automatic failover to alternate coordinator endpoints.
 *
 * @param endpoints         list of coordinator endpoints (host:port) for failover
 * @param healthCheckInterval interval between health check probes
 * @param connectionIdleTimeout idle timeout before connections are evicted
 * @param maxConnectionsPerEndpoint maximum concurrent connections per endpoint
 * @param failoverEnabled   whether automatic failover to alternate endpoints is enabled
 */
public record ConnectionPoolConfig(
        List<String> endpoints,
        Duration healthCheckInterval,
        Duration connectionIdleTimeout,
        int maxConnectionsPerEndpoint,
        boolean failoverEnabled
) {

    /** Default: single endpoint, 30s health check, 5min idle, 10 connections. */
    public static final Duration DEFAULT_HEALTH_CHECK_INTERVAL = Duration.ofSeconds(30);
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMinutes(5);
    public static final int DEFAULT_MAX_CONNECTIONS = 10;

    public ConnectionPoolConfig {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must contain at least one entry");
        }
        endpoints = List.copyOf(endpoints);
        requirePositiveDuration(healthCheckInterval, "healthCheckInterval");
        requirePositiveDuration(connectionIdleTimeout, "connectionIdleTimeout");
        if (maxConnectionsPerEndpoint < 1) {
            throw new IllegalArgumentException("maxConnectionsPerEndpoint must be >= 1, got "
                    + maxConnectionsPerEndpoint);
        }
    }

    private static void requirePositiveDuration(Duration timeout, String name) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    /**
     * Creates a single-endpoint configuration with defaults.
     *
     * @param host coordinator host
     * @param port coordinator port
     * @return default pool config for a single endpoint
     */
    public static ConnectionPoolConfig singleEndpoint(String host, int port) {
        return new ConnectionPoolConfig(
                List.of(host + ":" + port),
                DEFAULT_HEALTH_CHECK_INTERVAL,
                DEFAULT_IDLE_TIMEOUT,
                DEFAULT_MAX_CONNECTIONS,
                false
        );
    }

    /**
     * Creates a multi-endpoint configuration with failover enabled.
     *
     * @param endpoints list of host:port strings
     * @return pool config with failover enabled
     */
    public static ConnectionPoolConfig withFailover(List<String> endpoints) {
        return new ConnectionPoolConfig(
                endpoints,
                DEFAULT_HEALTH_CHECK_INTERVAL,
                DEFAULT_IDLE_TIMEOUT,
                DEFAULT_MAX_CONNECTIONS,
                true
        );
    }

    /**
     * Returns the primary endpoint (first in the list).
     */
    public String primaryEndpoint() {
        return endpoints.get(0);
    }

    /**
     * Returns alternate endpoints for failover (all except primary).
     */
    public List<String> alternateEndpoints() {
        if (endpoints.size() <= 1) {
            return List.of();
        }
        return endpoints.subList(1, endpoints.size());
    }
}
