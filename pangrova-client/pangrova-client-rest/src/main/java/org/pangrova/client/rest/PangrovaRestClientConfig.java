package org.pangrova.client.rest;

import java.time.Duration;

/**
 * Immutable configuration for {@link PangrovaRestClient}.
 * All fields are final; no setters. Construct via {@link PangrovaRestClient.Builder}.
 *
 * @param coordinatorHost Coordinator hostname or IP
 * @param coordinatorPort Coordinator HTTP port (1-65535)
 * @param connectTimeout  TCP connect timeout
 * @param requestTimeout  HTTP request timeout (covers full round-trip)
 * @param useTls          whether to use TLS for REST and transport connections
 * @param trustStorePath  path to JKS/PKCS12 trust store (null for system default)
 * @param trustStorePassword password for the trust store (null if none)
 * @param bearerToken     bearer token for Authorization header (null if none)
 * @param retryPolicy     retry policy for transient failures (null uses RetryPolicy.DEFAULT)
 * @param connectionPoolConfig connection pool and failover configuration (null disables pooling/failover)
 */
public record PangrovaRestClientConfig(
        String coordinatorHost,
        int coordinatorPort,
        Duration connectTimeout,
        Duration requestTimeout,
        boolean useTls,
        String trustStorePath,
        String trustStorePassword,
        String bearerToken,
        RetryPolicy retryPolicy,
        ConnectionPoolConfig connectionPoolConfig
) {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    /** Minimum valid TCP port. */
    public static final int MIN_PORT = 1;
    /** Maximum valid TCP port. */
    public static final int MAX_PORT = 65535;

    /** Backward-compatible constructor without TLS/auth/retry/pool fields. */
    public PangrovaRestClientConfig(String coordinatorHost, int coordinatorPort,
                                   Duration connectTimeout, Duration requestTimeout) {
        this(coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                false, null, null, null, RetryPolicy.DEFAULT, null);
    }

    /**
     * Backward-compatible constructor without retry/pool fields;
     * TLS settings are grouped in a {@link TlsSettings} record
     * (a null value disables TLS).
     */
    public PangrovaRestClientConfig(String coordinatorHost, int coordinatorPort,
                                   Duration connectTimeout, Duration requestTimeout,
                                   TlsSettings tls, String bearerToken) {
        this(coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                tls != null && tls.enabled(),
                tls != null ? tls.storePath() : null,
                tls != null ? tls.storePassword() : null,
                bearerToken, RetryPolicy.DEFAULT, null);
    }

    public PangrovaRestClientConfig {
        requireNonBlankHost(coordinatorHost);
        requireValidPort(coordinatorPort);
        requirePositiveDuration(connectTimeout, "connectTimeout");
        requirePositiveDuration(requestTimeout, "requestTimeout");
        if (retryPolicy == null) {
            retryPolicy = RetryPolicy.DEFAULT;
        }
    }

    public String scheme() {
        return useTls ? "https" : "http";
    }

    private static void requireNonBlankHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("coordinatorHost must not be null or blank");
        }
    }

    private static void requireValidPort(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException(
                    "coordinatorPort must be between 1 and 65535, got " + port);
        }
    }

    private static void requirePositiveDuration(Duration timeout, String name) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException(name + " must be a positive duration");
        }
    }

    /**
     * TLS connection settings grouping for the compact configuration constructor.
     *
     * @param enabled        whether TLS is enabled
     * @param storePath      path to JKS/PKCS12 trust store (null for system default)
     * @param storePassword  password for the trust store (null if none)
     */
    public record TlsSettings(boolean enabled, String storePath, String storePassword) {

        /** TLS-disabled settings. */
        public static final TlsSettings DISABLED = new TlsSettings(false, null, null);
    }
}
