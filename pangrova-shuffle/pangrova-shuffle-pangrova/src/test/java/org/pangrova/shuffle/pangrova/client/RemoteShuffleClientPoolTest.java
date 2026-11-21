package org.pangrova.shuffle.pangrova.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

import org.pangrova.shuffle.api.ShuffleServerEndpoint;
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
        pool = new RemoteShuffleClientPool(5000, 10000, RemoteShuffleClientPool.PoolOptions.defaults(), admission());
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void getOrCreateReturnsNonNullClient() {
        try (var lease = pool.borrow(endpoint("localhost", 9400))) {
            assertNotNull(lease.client());
            assertEquals(1, pool.activeConnectionCount());
        }
    }

    @Test
    void sameEndpointReturnsSameClient() {
        try (var first = pool.borrow(endpoint("localhost", 9400));
             var second = pool.borrow(endpoint("localhost", 9400))) {
            assertSame(first.client(), second.client(), "Same endpoint should return pooled client");
            assertEquals(1, pool.activeConnectionCount());
        }
    }

    @Test
    void differentEndpointsReturnDifferentClients() {
        try (var a = pool.borrow(endpoint("host-a", 9400));
             var b = pool.borrow(endpoint("host-b", 9401))) {
            assertNotNull(a.client());
            assertNotNull(b.client());
            assertEquals(2, pool.activeConnectionCount());
        }
    }

    @Test
    void closeDrainsAllEntries() {
        pool.borrow(endpoint("host-a", 9400)).close();
        pool.borrow(endpoint("host-b", 9401)).close();
        assertEquals(2, pool.activeConnectionCount());

        pool.close();
        assertEquals(0, pool.activeConnectionCount());
    }

    @Test
    void validatesConstructorParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new RemoteShuffleClientPool(0, 10000, RemoteShuffleClientPool.PoolOptions.defaults(), admission()));
        assertThrows(IllegalArgumentException.class,
                () -> new RemoteShuffleClientPool(5000, 0, RemoteShuffleClientPool.PoolOptions.defaults(), admission()));
        assertThrows(IllegalArgumentException.class,
                () -> new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(100, 0));
    }

    @Test
    void plaintextPoolRejectsTlsBoundEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> pool.borrow(
                        new ShuffleServerEndpoint(
                                "localhost", 9400, 0, 1, "shuffle.example")));
    }

    @Test
    void maxPoolSizeEvictsOldestEntries() {
        int maxPoolSize = 5;
        try (RemoteShuffleClientPool bounded = new RemoteShuffleClientPool(
                5000, 10000, new RemoteShuffleClientPool.PoolOptions(5, TimeUnit.MINUTES, maxPoolSize,
                        false, null, null), admission())) {
            for (int i = 0; i < maxPoolSize + 3; i++) {
                bounded.borrow(endpoint("host-" + i, 9400)).close();
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
                5000, 10000, new RemoteShuffleClientPool.PoolOptions(50, TimeUnit.MILLISECONDS, 100,
                        false, null, null), admission())) {
            shortLived.borrow(endpoint("localhost", 9400)).close();
            assertEquals(1, shortLived.activeConnectionCount());

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                shortLived.cleanUp();
                assertEquals(0, shortLived.activeConnectionCount(), "Idle channel must actually terminate");
            });
        }
    }

    @Test
    void accessResetsIdleTimer() throws Exception {
        try (RemoteShuffleClientPool shortLived = new RemoteShuffleClientPool(
                5000, 10000, new RemoteShuffleClientPool.PoolOptions(150, TimeUnit.MILLISECONDS, 100,
                        false, null, null), admission())) {
            PangrovaShuffleClient original;
            try (var lease = shortLived.borrow(endpoint("localhost", 9400))) { original = lease.client(); }
            await().pollInterval(Duration.ofMillis(30)).during(Duration.ofMillis(400))
                    .atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                        try (var lease = shortLived.borrow(endpoint("localhost", 9400))) {
                            assertSame(original, lease.client());
                            assertEquals(1, shortLived.activeConnectionCount());
                        }
                    });
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                shortLived.cleanUp();
                assertEquals(0, shortLived.activeConnectionCount());
                assertTrue(original.isTerminated());
            });
        }
    }

    @Test
    void defaultPoolUsesExpectedDefaults() {
        try (RemoteShuffleClientPool defaultPool = new RemoteShuffleClientPool(
                5000, 10000, RemoteShuffleClientPool.PoolOptions.defaults(), admission())) {
            try (var lease = defaultPool.borrow(endpoint("localhost", 9400))) { assertNotNull(lease.client()); }
            assertEquals(1, defaultPool.activeConnectionCount());
        }
    }

    @Test
    void activeBorrowerPreventsShutdownUntilActualLeaseReturn() {
        var lease = pool.borrow(endpoint("localhost", 9400));
        var client = lease.client();
        assertThrows(IllegalStateException.class, pool::close);
        assertTrue(!client.isTerminated());
        assertEquals(1, pool.activeConnectionCount());
        assertThrows(IllegalStateException.class, () -> pool.borrow(endpoint("localhost", 9400)));
        lease.close();
        pool.close();
        assertEquals(0, pool.activeConnectionCount());
        assertTrue(client.isTerminated());
    }

    private static ShuffleServerEndpoint endpoint(String host, int port) {
        return new ShuffleServerEndpoint(host, port, 0, 1, "");
    }

    private static RemoteShuffleChannelAdmission admission() {
        return new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(100, 8));
    }
}
