package org.pangrova.common.http;

import java.time.Duration;

/**
 * Configuration for shared HTTP client instances.
 * Used by RemoteTaskDispatcher, RemoteTaskStatusReporter, HeartbeatSender.
 *
 * @param name logical name used for thread naming ({name}-http-client-{n})
 * @param maxConnections maximum number of concurrent connections
 * @param connectTimeout TCP connect timeout
 * @param requestTimeout per-request timeout
 */
public record HttpClientConfig(
        String name,
        int maxConnections,
        Duration connectTimeout,
        Duration requestTimeout) {

    public static final HttpClientConfig DEFAULT = new HttpClientConfig(
            "default", 64, Duration.ofSeconds(5), Duration.ofSeconds(30));

    public HttpClientConfig {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("maxConnections must be > 0");
        }
        if (connectTimeout == null) {
            throw new IllegalArgumentException("connectTimeout must not be null");
        }
        if (requestTimeout == null) {
            throw new IllegalArgumentException("requestTimeout must not be null");
        }
    }
}
