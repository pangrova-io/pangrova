package org.pangrova.coordinator.transport;

import org.apache.arrow.memory.BufferAllocator;

import java.util.Objects;

/**
 * Immutable configuration for {@link TransportClient}, eliminating
 * telescoping constructors.
 *
 * <p>If {@code allocator} is null, TransportClient creates and owns
 * a RootAllocator internally. If provided, the caller retains ownership.</p>
 */
public record TransportClientConfig(
        String host,
        int port,
        long maxResultBytes,
        BufferAllocator allocator) {

    /** Default max result size: 256 MB. */
    public static final long DEFAULT_MAX_RESULT_BYTES = 256L * 1024 * 1024;

    public TransportClientConfig {
        Objects.requireNonNull(host, "host must not be null");
        if (port < 0) {
            throw new IllegalArgumentException("port must be non-negative");
        }
        if (maxResultBytes <= 0) {
            throw new IllegalArgumentException("maxResultBytes must be positive");
        }
    }

    /** Minimal config: host + port with defaults. */
    public static TransportClientConfig of(String host, int port) {
        return new TransportClientConfig(host, port, DEFAULT_MAX_RESULT_BYTES, null);
    }

    /** Config with custom result size limit. */
    public static TransportClientConfig of(String host, int port, long maxResultBytes) {
        return new TransportClientConfig(host, port, maxResultBytes, null);
    }

    /** Config with external allocator. */
    public static TransportClientConfig of(String host, int port, BufferAllocator allocator, long maxResultBytes) {
        return new TransportClientConfig(host, port, maxResultBytes, allocator);
    }
}
