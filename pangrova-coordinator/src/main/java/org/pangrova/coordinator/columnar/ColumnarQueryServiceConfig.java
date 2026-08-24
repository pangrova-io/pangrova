package org.pangrova.coordinator.columnar;

import java.util.Objects;

/**
 * Immutable configuration for the columnar query service (columnar SQL endpoint).
 * Controls bind address, port, and connection limits.
 */
public record ColumnarQueryServiceConfig(
        String bindHost,
        int port,
        int maxConcurrentStreams,
        long preparedStatementTtlMs) {

    public static final String DEFAULT_BIND_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 32010;
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS = 256;
    public static final long DEFAULT_PREPARED_STATEMENT_TTL_MS = 300_000L;
    private static final int MAX_PORT = 65_535;

    public ColumnarQueryServiceConfig {
        Objects.requireNonNull(bindHost, "bindHost");
        if (port < 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("port must be 0-65535, got: " + port);
        }
        if (maxConcurrentStreams <= 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentStreams must be positive, got: " + maxConcurrentStreams);
        }
        if (preparedStatementTtlMs <= 0) {
            throw new IllegalArgumentException(
                    "preparedStatementTtlMs must be positive, got: " + preparedStatementTtlMs);
        }
    }

    /** Returns a configuration instance with default values. */
    public static ColumnarQueryServiceConfig defaults() {
        return new ColumnarQueryServiceConfig(
                DEFAULT_BIND_HOST,
                DEFAULT_PORT,
                DEFAULT_MAX_CONCURRENT_STREAMS,
                DEFAULT_PREPARED_STATEMENT_TTL_MS);
    }
}
