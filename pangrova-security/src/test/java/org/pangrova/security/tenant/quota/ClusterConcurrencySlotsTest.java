package org.pangrova.security.tenant.quota;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-ADMISSION-01: verifies the atomic cluster-wide concurrency gate never oversubscribes a tenant's
 * limit, including under concurrent reservation from multiple "coordinators" sharing one store.
 */
class ClusterConcurrencySlotsTest {

    private static DataSource h2(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    @Test
    void jdbcReserveRespectsLimit() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(h2("slots_limit"));
        assertThat(slots.tryReserve("t", 2)).isTrue();
        assertThat(slots.tryReserve("t", 2)).isTrue();
        assertThat(slots.tryReserve("t", 2)).as("3rd exceeds limit 2").isFalse();
        assertThat(slots.active("t")).isEqualTo(2);
    }

    @Test
    void jdbcReleaseFreesSlot() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(h2("slots_release"));
        assertThat(slots.tryReserve("t", 1)).isTrue();
        assertThat(slots.tryReserve("t", 1)).as("at limit").isFalse();
        slots.release("t");
        assertThat(slots.active("t")).isZero();
        assertThat(slots.tryReserve("t", 1)).as("slot freed after release").isTrue();
    }

    @Test
    void jdbcConcurrentReservesFromTwoCoordinatorsNeverOversell() throws InterruptedException {
        // Two coordinators sharing ONE store burst-admit; the atomic gate must cap total at the limit.
        DataSource shared = h2("slots_concurrent");
        ClusterConcurrencySlots coordA = new JdbcClusterConcurrencySlots(shared);
        ClusterConcurrencySlots coordB = new JdbcClusterConcurrencySlots(shared);
        int limit = 5;
        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger reserved = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) {
                ClusterConcurrencySlots coord = (i % 2 == 0) ? coordA : coordB;
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (coord.tryReserve("t", limit)) {
                        reserved.incrementAndGet();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(reserved.get()).as("no cross-coordinator oversell").isEqualTo(limit);
        assertThat(coordA.active("t")).as("store reflects exactly the limit").isEqualTo(limit);
    }

    @Test
    void inMemoryReserveRespectsLimitAndRelease() {
        ClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
        assertThat(slots.tryReserve("t", 1)).isTrue();
        assertThat(slots.tryReserve("t", 1)).isFalse();
        slots.release("t");
        assertThat(slots.tryReserve("t", 1)).isTrue();
        assertThat(slots.active("t")).isEqualTo(1);
    }

    @Test
    void inMemoryConcurrentReservesNeverOversell() throws InterruptedException {
        ClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
        int limit = 3;
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger reserved = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (slots.tryReserve("t", limit)) {
                        reserved.incrementAndGet();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(reserved.get()).isEqualTo(limit);
    }
}
