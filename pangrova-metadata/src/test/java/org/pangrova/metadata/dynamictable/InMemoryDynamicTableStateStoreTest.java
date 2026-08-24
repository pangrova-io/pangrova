package org.pangrova.metadata.dynamictable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InMemoryDynamicTableStateStore}.
 *
 * <p><b>Validates: Requirements 2, 7, 13, 14</b></p>
 */
class InMemoryDynamicTableStateStoreTest {

    private InMemoryDynamicTableStateStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryDynamicTableStateStore();
    }

    @Test
    void saveAndLoadState() {
        DynamicTableState state = createState("dt_sales", "t1", DynamicTableLifecycleState.ACTIVE);
        store.saveState(state);

        Optional<DynamicTableState> loaded = store.loadState("dt_sales");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().dtName()).isEqualTo("dt_sales");
        assertThat(loaded.get().tenantId()).isEqualTo("t1");
        assertThat(loaded.get().state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
    }

    @Test
    void loadStateReturnsEmptyForMissing() {
        assertThat(store.loadState("nonexistent")).isEmpty();
    }

    @Test
    void saveStateOverwritesExisting() {
        store.saveState(createState("dt_orders", "t1", DynamicTableLifecycleState.CREATING));
        store.saveState(createState("dt_orders", "t1", DynamicTableLifecycleState.ACTIVE));

        Optional<DynamicTableState> loaded = store.loadState("dt_orders");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
    }

    @Test
    void deleteExistingState() {
        store.saveState(createState("dt_temp", "t1", DynamicTableLifecycleState.ACTIVE));
        assertThat(store.delete("dt_temp")).isTrue();
        assertThat(store.loadState("dt_temp")).isEmpty();
    }

    @Test
    void deleteNonExistentReturnsFalse() {
        assertThat(store.delete("nonexistent")).isFalse();
    }

    @Test
    void listAllReturnsAllStates() {
        store.saveState(createState("dt_a", "t1", DynamicTableLifecycleState.ACTIVE));
        store.saveState(createState("dt_b", "t2", DynamicTableLifecycleState.SUSPENDED));
        store.saveState(createState("dt_c", "t1", DynamicTableLifecycleState.DROPPED));

        List<DynamicTableState> all = store.listAll();
        assertThat(all).hasSize(3);
    }

    @Test
    void listActiveByTenantFiltersCorrectly() {
        store.saveState(createState("dt_a", "t1", DynamicTableLifecycleState.ACTIVE));
        store.saveState(createState("dt_b", "t1", DynamicTableLifecycleState.SUSPENDED));
        store.saveState(createState("dt_c", "t1", DynamicTableLifecycleState.ACTIVE));
        store.saveState(createState("dt_d", "t2", DynamicTableLifecycleState.ACTIVE));

        List<DynamicTableState> t1Active = store.listActiveByTenant("t1");
        assertThat(t1Active).hasSize(2);
        assertThat(t1Active).allMatch(s -> s.tenantId().equals("t1"));
        assertThat(t1Active).allMatch(s -> s.state() == DynamicTableLifecycleState.ACTIVE);
    }

    @Test
    void listActiveByTenantReturnsEmptyForNoMatch() {
        store.saveState(createState("dt_a", "t1", DynamicTableLifecycleState.SUSPENDED));
        assertThat(store.listActiveByTenant("t1")).isEmpty();
        assertThat(store.listActiveByTenant("t_unknown")).isEmpty();
    }

    @Test
    void concurrentSaveAndLoadIsSafe() throws InterruptedException {
        int threadCount = 8;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String name = "dt_" + threadId + "_" + i;
                        store.saveState(createState(name, "t" + threadId, DynamicTableLifecycleState.ACTIVE));
                        store.loadState(name);
                        store.listAll();
                        store.listActiveByTenant("t" + threadId);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        assertThat(errors.get()).isZero();
        assertThat(store.listAll()).hasSize(threadCount * opsPerThread);
    }

    private static DynamicTableState createState(String dtName, String tenantId, DynamicTableLifecycleState state) {
        Instant now = Instant.now();
        return new DynamicTableState(
                dtName, tenantId, state,
                DynamicTableRefreshMode.SNAPSHOT, Duration.ofMinutes(5),
                "snap-0", "SNAPSHOT",
                1, null,
                "admin", "coord-1", 1L,
                now, 0L, 0,
                now, now);
    }
}
