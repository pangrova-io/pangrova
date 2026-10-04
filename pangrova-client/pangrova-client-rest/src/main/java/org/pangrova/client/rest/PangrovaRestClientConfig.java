package org.pangrova.client.rest;

import org.pangrova.client.rest.auth.ClientAuthentication;
import org.pangrova.transport.security.ResultTransportSecurity;

import java.time.Duration;

/**
 * Immutable configuration for {@link PangrovaRestClient}.
 * All fields are final; no setters. Construct via {@link PangrovaRestClient.Builder}.
 *
 * <p>Connect and request timeouts must not exceed {@code Long.MAX_VALUE / 2}
 * milliseconds. This supported JDK HTTP timer window reserves signed-long
 * millisecond arithmetic headroom; it is not a guarantee against arbitrary
 * wall-clock jumps. Timeout durations need not fit signed-long nanoseconds.</p>
 *
 * @param coordinatorHost Coordinator hostname or IP
 * @param coordinatorPort Coordinator HTTP port (1-65535)
 * @param connectTimeout  TCP connect timeout
 * @param requestTimeout  HTTP request timeout (covers full round-trip)
 * @param useTls          whether to use TLS for REST and transport connections
 * @param trustStorePath  REST HTTPS JKS/PKCS12 trust store (null for system default)
 * @param trustStorePassword password for the trust store (null if none)
 * @param authentication typed authentication for the Authorization header (null if none)
 * @param retryPolicy     retry policy for transient failures (null uses RetryPolicy.DEFAULT)
 * @param connectionPoolConfig connection pool and failover configuration (null disables pooling/failover)
 * @param httpExecutorThreads owned HTTP executor size, independent of endpoints (null uses pool/default executor)
 * @param resultTransportSecurity result-channel PEM policy (null uses system trust); useTls also requires TLS
 */
public record PangrovaRestClientConfig(
        String coordinatorHost,
        int coordinatorPort,
        Duration connectTimeout,
        Duration requestTimeout,
        boolean useTls,
        String trustStorePath,
        String trustStorePassword,
        ClientAuthentication authentication,
        RetryPolicy retryPolicy,
        ConnectionPoolConfig connectionPoolConfig,
        Integer httpExecutorThreads,
        ResultTransportSecurity resultTransportSecurity
) {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration MAX_HTTP_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE / 2);
    /** Minimum valid TCP port. */
    public static final int MIN_PORT = 1;
    /** Maximum valid TCP port. */
    public static final int MAX_PORT = 65535;

    /** Configuration using default security, retry and pooling policies. */
    public PangrovaRestClientConfig(String coordinatorHost, int coordinatorPort,
                                   Duration connectTimeout, Duration requestTimeout) {
        this(coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                false, null, null, null, RetryPolicy.DEFAULT, null, null, ResultTransportSecurity.systemTrust(false));
    }

    /**
     * Configuration using default retry/pool and result-channel trust policies;
     * TLS settings are grouped in a {@link TlsSettings} record
     * (a null value disables TLS).
     */
    public PangrovaRestClientConfig(String coordinatorHost, int coordinatorPort,
                                   Duration connectTimeout, Duration requestTimeout,
                                   TlsSettings tls, ClientAuthentication authentication) {
        this(coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                tls != null && tls.enabled(),
                tls != null ? tls.storePath() : null,
                tls != null ? tls.storePassword() : null,
                authentication, RetryPolicy.DEFAULT, null, null,
                ResultTransportSecurity.systemTrust(tls != null && tls.enabled()));
    }

    public PangrovaRestClientConfig {
        requireNonBlankHost(coordinatorHost);
        requireValidPort(coordinatorPort);
        requirePositiveDuration(connectTimeout, "connectTimeout");
        requirePositiveDuration(requestTimeout, "requestTimeout");
        if (httpExecutorThreads != null && httpExecutorThreads < 1) {
            throw new IllegalArgumentException("httpExecutorThreads must be positive");
        }
        if (trustStorePath != null && (trustStorePath.isBlank() || !useTls)) {
            throw new IllegalArgumentException("A non-blank trust store path requires HTTPS");
        }
        if (trustStorePassword != null && trustStorePath == null) {
            throw new IllegalArgumentException("A trust store password requires its trust store path");
        }
        if (retryPolicy == null) {
            retryPolicy = RetryPolicy.DEFAULT;
        }
        resultTransportSecurity = resultTransportSecurity == null
                ? ResultTransportSecurity.systemTrust(useTls) : resultTransportSecurity.requiringTls(useTls);
    }

    public String scheme() {
        return useTls ? "https" : "http";
    }

    @Override
    public String toString() {
        return "PangrovaRestClientConfig[coordinatorHost=" + coordinatorHost
                + ", coordinatorPort=" + coordinatorPort + ", useTls=" + useTls
                + ", authentication=" + authentication + ", trustStorePassword=<redacted>"
                + ", resultTransportSecurity=" + resultTransportSecurity + "]";
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
        if (timeout.compareTo(MAX_HTTP_TIMEOUT) > 0) {
            throw new IllegalArgumentException(name + " must not exceed "
                    + (Long.MAX_VALUE / 2) + " milliseconds");
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
