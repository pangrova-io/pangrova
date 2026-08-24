package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;

/**
 * Factory SPI for creating {@link DistributedCacheStore} instances based
 * on configuration. Each backend (Redis, shared filesystem, object store)
 * provides its own factory implementation discovered via service loading.
 *
 * <h3>Configuration model</h3>
 * <p>Configuration is passed as a flat key-value map extracted from the
 * worker YAML config under the {@code cache.distributed} prefix.
 * Backend-specific keys are namespaced by the backend name
 * (e.g. {@code redis.url}, {@code redis.ttl-seconds}).</p>
 *
 * <h3>Example configuration (worker.yml)</h3>
 * <pre>{@code
 * cache:
 *   topology: SHARED_STORE
 *   distributed:
 *     backend: redis
 *     redis.url: redis://cache-cluster:6379
 *     redis.ttl-seconds: 3600
 *     redis.max-entry-bytes: 67108864
 * }</pre>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 */
@Experimental(reason = "G-CACHE-08: distributed cache store factory SPI")
public interface DistributedCacheStoreFactory {

    /**
     * Returns the backend name that this factory handles (e.g. "redis",
     * "shared-fs", "s3-tier"). Must be unique across all registered
     * factories. Matched against the {@code cache.distributed.backend}
     * config value.
     *
     * @return the backend name, never null or empty
     */
    String backendName();

    /**
     * Creates a new cache store instance from the given configuration.
     *
     * <p>Called once during worker startup. The returned store is used
     * for the lifetime of the worker process. The caller is responsible
     * for calling {@link DistributedCacheStore#close()} on shutdown.</p>
     *
     * @param config configuration properties (backend-specific keys
     *               such as {@code redis.url}, {@code redis.ttl-seconds})
     * @return a configured, ready-to-use cache store
     * @throws IllegalArgumentException if required configuration is missing
     */
    DistributedCacheStore create(Map<String, String> config);
}
