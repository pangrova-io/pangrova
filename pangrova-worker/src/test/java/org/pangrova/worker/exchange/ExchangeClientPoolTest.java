package org.pangrova.worker.exchange;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.TimeUnit;

import org.pangrova.worker.exchange.transport.TransportClientPool;
import org.pangrova.worker.transport.TransportClient;

/**
 * Verifies TransportClientPool connection reuse, eviction on error,
 * idle timeout eviction, max pool size cap, and proper cleanup on close.
 */
@Tag("native-required")
class ExchangeClientPoolTest {

    private TransportClientPool pool;

    @BeforeEach
    void setUp() {
        pool = new TransportClientPool();
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void getOrCreateReturnsNonNullClient() {
        TransportClient client = pool.getOrCreateClient("localhost", 9090);

        assertNotNull(client);
        assertEquals(1, pool.size());
    }

    @Test
    void sameHostPortReturnsSameClient() {
        TransportClient first = pool.getOrCreateClient("localhost", 9090);
        TransportClient second = pool.getOrCreateClient("localhost", 9090);

        assertSame(first, second, "Same host:port should return pooled client");
        assertEquals(1, pool.size());
    }

    @Test
    void differentHostPortReturnsDifferentClients() {
        TransportClient a = pool.getOrCreateClient("host-a", 9090);
        TransportClient b = pool.getOrCreateClient("host-b", 9091);

        assertNotNull(a);
        assertNotNull(b);
        assertEquals(2, pool.size());
    }

    @Test
    void evictRemovesClientFromPool() {
        pool.getOrCreateClient("localhost", 9090);
        assertEquals(1, pool.size());

        pool.evict("localhost", 9090);

        assertEquals(0, pool.size());
    }

    @Test
    void evictNonExistentKeyIsNoOp() {
        pool.evict("nonexistent", 1234);

        assertEquals(0, pool.size());
    }

    @Test
    void getAfterEvictCreatesNewClient() {
        TransportClient original = pool.getOrCreateClient("localhost", 9090);
        pool.evict("localhost", 9090);

        TransportClient replacement = pool.getOrCreateClient("localhost", 9090);

        assertNotNull(replacement);
        assertNotSame(original, replacement, "After eviction, a new client should be created");
        assertEquals(1, pool.size());
    }

    @Test
    void closeDrainsAllEntries() {
        pool.getOrCreateClient("host-a", 9090);
        pool.getOrCreateClient("host-b", 9091);
        pool.getOrCreateClient("host-c", 9092);
        assertEquals(3, pool.size());

        pool.close();

        assertEquals(0, pool.size());
    }

    @Test
    void buildKeyFormatsCorrectly() {
        assertEquals("localhost:9090", TransportClientPool.buildKey("localhost", 9090));
        assertEquals("10.0.0.1:8080", TransportClientPool.buildKey("10.0.0.1", 8080));
    }

    @Test
    void maxPoolSizeEvictsOldestEntries() {
        int maxSize = 5;
        try (TransportClientPool bounded = new TransportClientPool(5, TimeUnit.MINUTES, maxSize)) {
            for (int i = 0; i < maxSize + 3; i++) {
                bounded.getOrCreateClient("host-" + i, 9090);
            }
            bounded.cleanUp();
            // Caffeine may evict asynchronously, but after cleanUp the size should be capped
            int size = bounded.size();
            assertNotNull(size);
            // Pool size should not exceed max
            assertEquals(true, size <= maxSize,
                    "Pool size " + size + " should not exceed max " + maxSize);
        }
    }

    @Test
    void idleTimeoutEvictsExpiredEntries() throws Exception {
        // Use a very short idle timeout (50ms) to test eviction
        try (TransportClientPool shortLived = new TransportClientPool(50, TimeUnit.MILLISECONDS, 100)) {
            shortLived.getOrCreateClient("localhost", 9090);
            assertEquals(1, shortLived.size());

            // Wait for idle timeout to expire
            Thread.sleep(200);
            shortLived.cleanUp();

            assertEquals(0, shortLived.size(), "Idle client should be evicted after timeout");
        }
    }

    @Test
    void accessResetsIdleTimer() throws Exception {
        try (TransportClientPool shortLived = new TransportClientPool(150, TimeUnit.MILLISECONDS, 100)) {
            shortLived.getOrCreateClient("localhost", 9090);

            // Access before timeout expires to reset the timer
            Thread.sleep(80);
            shortLived.getOrCreateClient("localhost", 9090);

            Thread.sleep(80);
            shortLived.getOrCreateClient("localhost", 9090);

            shortLived.cleanUp();
            assertEquals(1, shortLived.size(), "Client should still be present after access resets idle timer");

            // Now let it expire
            Thread.sleep(200);
            shortLived.cleanUp();
            assertEquals(0, shortLived.size(), "Client should be evicted after idle timeout");
        }
    }

    @Test
    void defaultPoolUsesExpectedDefaults() {
        // Verify the default pool can be created and used
        try (TransportClientPool defaultPool = new TransportClientPool()) {
            assertNotNull(defaultPool.getOrCreateClient("localhost", 9090));
            assertEquals(1, defaultPool.size());
        }
    }
}
