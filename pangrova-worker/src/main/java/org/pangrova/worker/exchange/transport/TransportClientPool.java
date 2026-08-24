package org.pangrova.worker.exchange.transport;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.worker.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared connection pool for transport clients, keyed by {@code host:port}.
 *
 * <p>Backed by Caffeine cache with idle timeout (5 minutes) and maximum size (100).
 * Connections idle longer than the timeout are automatically evicted and closed.
 * Implements {@link AutoCloseable} to drain all pooled connections on Worker shutdown.</p>
 *
 * <p>Concurrency strategy: Caffeine cache handles thread-safe access internally.
 * Eviction callbacks run outside any cache lock, so connection close (I/O) never
 * holds a lock.</p>
 */
@ThreadSafe
public final class TransportClientPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransportClientPool.class);

    static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 5;
    static final int DEFAULT_MAX_SIZE = 100;

    private final Cache<String, TransportClient> pool;

    public TransportClientPool() {
        this(DEFAULT_IDLE_TIMEOUT_MINUTES, TimeUnit.MINUTES, DEFAULT_MAX_SIZE);
    }

    public TransportClientPool(long idleTimeout, TimeUnit timeUnit, int maxSize) {
        this.pool = Caffeine.newBuilder()
                .expireAfterAccess(idleTimeout, timeUnit)
                .maximumSize(maxSize)
                .removalListener(this::onRemoval)
                .build();
    }

    /**
     * Removal listener — called by Caffeine outside any cache lock.
     * Closes the evicted transport client. Safe for I/O: no lock held.
     */
    private void onRemoval(String key, TransportClient client, RemovalCause cause) {
        if (client == null) {
            return;
        }
        ResourceCloser.closeQuietly(client);
        LOG.info("event=transport_client_pool_evict key={} cause={}", key, cause);
    }

    /**
     * Returns a pooled TransportClient for the given host and port,
     * creating one if absent. Accessing a client resets its idle timer.
     */
    public TransportClient getOrCreateClient(String host, int port) {
        checkNotNull(host, "host must not be null");
        String key = buildKey(host, port);
        return pool.get(key, k -> {
            LOG.info("event=transport_client_pool_create key={}", k);
            return new TransportClient(host, port);
        });
    }

    /**
     * Evicts and closes the pooled client for the given host and port.
     * Called on connection failure to force a fresh connection on next access.
     */
    public void evict(String host, int port) {
        String key = buildKey(host, port);
        pool.invalidate(key);
    }

    /**
     * Returns the current number of pooled connections.
     * Note: Caffeine eviction is asynchronous, so this is an approximation.
     */
    public int size() {
        pool.cleanUp();
        return (int) pool.estimatedSize();
    }

    /**
     * Forces pending eviction maintenance to run synchronously.
     * Useful in tests to ensure eviction callbacks have completed.
     */
    public void cleanUp() {
        pool.cleanUp();
    }

    /**
     * Drains and closes all pooled TransportClient connections.
     * Idempotent — Caffeine invalidation triggers the removal listener for each entry.
     */
    @Override
    public void close() {
        LOG.info("event=transport_client_pool_drain size={}", pool.estimatedSize());
        pool.invalidateAll();
        pool.cleanUp();
        LOG.info("event=transport_client_pool_drained");
    }

    public static String buildKey(String host, int port) {
        return host + ":" + port;
    }
}
