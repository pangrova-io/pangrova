package org.pangrova.worker.exchange.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies RemoteShuffleClientPool connection reuse, idle timeout eviction,
 * max pool size cap, and proper cleanup on close.
 */
class RemoteShuffleClientPoolTest {

    private RemoteShuffleClientPool pool;

    @BeforeEach
    void setUp() {
        pool = new RemoteShuffleClientPool(5000, 10000, 8);
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void getOrCreateReturnsNonNullClient() {
        RemoteShuffleClient client = pool.getOrCreateClient("localhost", 9400);
        assertNotNull(client);
        assertEquals(1, pool.activeConnectionCount());
    }

    @Test
    void sameEndpointReturnsSameClient() {
        RemoteShuffleClient first = pool.getOrCreateClient("localhost", 9400);
        RemoteShuffleClient second = pool.getOrCreateClient("localhost", 9400);
        assertSame(first, second, "Same endpoint should return pooled client");
        assertEquals(1, pool.activeConnectionCount());
    }

    @Test
    void differentEndpointsReturnDifferentClients() {
        RemoteShuffleClient a = pool.getOrCreateClient("host-a", 9400);
        RemoteShuffleClient b = pool.getOrCreateClient("host-b", 9401);
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(2, pool.activeConnectionCount());
    }

    @Test
    void closeDrainsAllEntries() {
        pool.getOrCreateClient("host-a", 9400);
        pool.getOrCreateClient("host-b", 9401);
        assertEquals(2, pool.activeConnectionCount());

        pool.close();
        assertEquals(0, pool.activeConnectionCount());
    }

    @Test
    void validatesConstructorParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new RemoteShuffleClientPool(0, 10000, 8));
        assertThrows(IllegalArgumentException.class,
                () -> new RemoteShuffleClientPool(5000, 0, 8));
        assertThrows(IllegalArgumentException.class,
                () -> new RemoteShuffleClientPool(5000, 10000, 0));
    }

    @Test
    void maxPoolSizeEvictsOldestEntries() {
        int maxPoolSize = 5;
        try (RemoteShuffleClientPool bounded = new RemoteShuffleClientPool(
                5000, 10000, 8, 5, TimeUnit.MINUTES, maxPoolSize, false)) {
            for (int i = 0; i < maxPoolSize + 3; i++) {
                bounded.getOrCreateClient("host-" + i, 9400);
            }
            bounded.cleanUp();
            int size = bounded.activeConnectionCount();
            assertTrue(size <= maxPoolSize,
                    "Pool size " + size + " should not exceed max " + maxPoolSize);
        }
    }

    @Test
    void idleTimeoutEvictsExpiredEntries() throws Exception {
        try (RemoteShuffleClientPool shortLived = new RemoteShuffleClientPool(
                5000, 10000, 8, 50, TimeUnit.MILLISECONDS, 100, false)) {
            shortLived.getOrCreateClient("localhost", 9400);
            assertEquals(1, shortLived.activeConnectionCount());

            Thread.sleep(200);
            shortLived.cleanUp();

            assertEquals(0, shortLived.activeConnectionCount(),
                    "Idle client should be evicted after timeout");
        }
    }

    @Test
    void accessResetsIdleTimer() throws Exception {
        try (RemoteShuffleClientPool shortLived = new RemoteShuffleClientPool(
                5000, 10000, 8, 150, TimeUnit.MILLISECONDS, 100, false)) {
            shortLived.getOrCreateClient("localhost", 9400);

            // Access before timeout to reset idle timer
            Thread.sleep(80);
            shortLived.getOrCreateClient("localhost", 9400);

            Thread.sleep(80);
            shortLived.getOrCreateClient("localhost", 9400);

            shortLived.cleanUp();
            assertEquals(1, shortLived.activeConnectionCount(),
                    "Client should still be present after access resets idle timer");

            // Now let it expire
            Thread.sleep(200);
            shortLived.cleanUp();
            assertEquals(0, shortLived.activeConnectionCount(),
                    "Client should be evicted after idle timeout");
        }
    }

    @Test
    void defaultPoolUsesExpectedDefaults() {
        try (RemoteShuffleClientPool defaultPool = new RemoteShuffleClientPool(5000, 10000, 8)) {
            assertNotNull(defaultPool.getOrCreateClient("localhost", 9400));
            assertEquals(1, defaultPool.activeConnectionCount());
        }
    }
}
