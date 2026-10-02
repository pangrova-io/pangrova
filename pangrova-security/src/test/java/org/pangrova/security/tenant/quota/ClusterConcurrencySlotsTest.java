package org.pangrova.security.tenant.quota;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;
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

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_security_test")
                    .withUsername("pangrova")
                    .withPassword("pangrova");

    @BeforeAll
    static void startPostgres() {
        POSTGRES.start();
    }

    @AfterAll
    static void stopPostgres() {
        POSTGRES.stop();
    }

    private static DataSource postgres(String name) {
        String schema = name.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]", "_") + "_"
                + UUID.randomUUID().toString().replace("-", "");
        PGSimpleDataSource admin = dataSource(null);
        try (var connection = admin.getConnection();
             var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create isolated test schema", e);
        }
        return dataSource(schema);
    }

    private static PGSimpleDataSource dataSource(String schema) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        if (schema != null) {
            dataSource.setCurrentSchema(schema);
        }
        return dataSource;
    }

    @Test
    void jdbcReserveRespectsLimit() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(
                postgres("slots_limit"));
        assertThat(slots.tryReserve("t", 2)).isTrue();
        assertThat(slots.tryReserve("t", 2)).isTrue();
        assertThat(slots.tryReserve("t", 2)).as("3rd exceeds limit 2").isFalse();
        assertThat(slots.active("t")).isEqualTo(2);
    }

    @Test
    void jdbcReleaseFreesSlot() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(
                postgres("slots_release"));
        assertThat(slots.tryReserve("t", 1)).isTrue();
        assertThat(slots.tryReserve("t", 1)).as("at limit").isFalse();
        slots.release("t");
        assertThat(slots.active("t")).isZero();
        assertThat(slots.tryReserve("t", 1)).as("slot freed after release").isTrue();
    }

    @Test
    void jdbcConcurrentReservesFromTwoCoordinatorsNeverOversell() throws InterruptedException {
        // Two coordinators sharing ONE store burst-admit; the atomic gate must cap total at the limit.
        DataSource shared = postgres("slots_concurrent");
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
