package org.pangrova.coordinator.http;

/**
 * Immutable configuration for request body size and per-IP rate limiting.
 *
 * @param maxRequestSizeBytes maximum allowed request body size in bytes (must be positive)
 * @param maxRequestsPerMinute maximum requests per minute per IP (must be positive)
 */
public record RequestLimitConfig(int maxRequestSizeBytes, int maxRequestsPerMinute) {

    /** Default: 1 MB request body limit. */
    public static final int DEFAULT_MAX_REQUEST_SIZE_BYTES = 1_048_576;

    /** Default: 100 requests per minute per IP. */
    public static final int DEFAULT_MAX_REQUESTS_PER_MINUTE = 100;

    public RequestLimitConfig {
        if (maxRequestSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxRequestSizeBytes must be positive, got: " + maxRequestSizeBytes);
        }
        if (maxRequestsPerMinute <= 0) {
            throw new IllegalArgumentException(
                    "maxRequestsPerMinute must be positive, got: " + maxRequestsPerMinute);
        }
    }

    /**
     * Creates a config with default values: 1 MB body limit, 100 requests/minute per IP.
     */
    public static RequestLimitConfig defaults() {
        return new RequestLimitConfig(DEFAULT_MAX_REQUEST_SIZE_BYTES, DEFAULT_MAX_REQUESTS_PER_MINUTE);
    }
}
