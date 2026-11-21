package org.pangrova.worker.resource.tenant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TenantResourceTracker}.
 */
class TenantResourceTrackerTest {

    private TenantResourceTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new TenantResourceTracker();
    }

    @Test
    void unlimitedQuota_alwaysAdmits() {
        // No quota set — defaults to UNLIMITED
        assertThat(tracker.tryAcquireQuery("t1")).isTrue();
        assertThat(tracker.tryAllocateMemory("t1", 1_000_000)).isTrue();
    }

    @Test
    void memoryQuota_rejectsOverLimit() {
        tracker.setQuota("t1", new TenantResourceQuota(1024, -1));

        assertThat(tracker.tryAllocateMemory("t1", 512)).isTrue();
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(512);

        // This would exceed 1024
        assertThat(tracker.tryAllocateMemory("t1", 600)).isFalse();
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(512);

        // Exact fit
        assertThat(tracker.tryAllocateMemory("t1", 512)).isTrue();
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(1024);
    }

    @Test
    void memoryRelease_allowsSubsequentAllocation() {
        tracker.setQuota("t1", new TenantResourceQuota(1024, -1));

        assertThat(tracker.tryAllocateMemory("t1", 1024)).isTrue();
        assertThat(tracker.tryAllocateMemory("t1", 1)).isFalse();

        tracker.releaseMemory("t1", 512);
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(512);
        assertThat(tracker.tryAllocateMemory("t1", 512)).isTrue();
    }

    @Test
    void concurrencyQuota_rejectsOverLimit() {
        tracker.setQuota("t1", new TenantResourceQuota(-1, 2));

        assertThat(tracker.tryAcquireQuery("t1")).isTrue();
        assertThat(tracker.tryAcquireQuery("t1")).isTrue();
        assertThat(tracker.tryAcquireQuery("t1")).isFalse();
        assertThat(tracker.getActiveQueryCount("t1")).isEqualTo(2);
    }

    @Test
    void queryRelease_allowsNewQuery() {
        tracker.setQuota("t1", new TenantResourceQuota(-1, 1));

        assertThat(tracker.tryAcquireQuery("t1")).isTrue();
        assertThat(tracker.tryAcquireQuery("t1")).isFalse();

        tracker.releaseQuery("t1");
        assertThat(tracker.tryAcquireQuery("t1")).isTrue();
    }

    @Test
    void isOverMemoryQuota_reflectsState() {
        tracker.setQuota("t1", new TenantResourceQuota(100, -1));

        assertThat(tracker.isOverMemoryQuota("t1")).isFalse();
        tracker.tryAllocateMemory("t1", 100);
        assertThat(tracker.isOverMemoryQuota("t1")).isFalse(); // exactly at limit
    }

    @Test
    void differentTenants_isolatedQuotas() {
        tracker.setQuota("t1", new TenantResourceQuota(100, 1));
        tracker.setQuota("t2", new TenantResourceQuota(200, 2));

        assertThat(tracker.tryAllocateMemory("t1", 100)).isTrue();
        assertThat(tracker.tryAllocateMemory("t1", 1)).isFalse();

        // t2 is independent
        assertThat(tracker.tryAllocateMemory("t2", 200)).isTrue();
        assertThat(tracker.tryAcquireQuery("t2")).isTrue();
        assertThat(tracker.tryAcquireQuery("t2")).isTrue();
        assertThat(tracker.tryAcquireQuery("t2")).isFalse();
    }

    @Test
    void limitedMemoryCannotWrapCounterAndBypassTheQuota() {
        long limit = Long.MAX_VALUE - 16;
        tracker.setQuota("t1", new TenantResourceQuota(limit, -1));
        assertThat(tracker.tryAllocateMemory("t1", limit - 8)).isTrue();
        assertThat(tracker.tryAllocateMemory("t1", 32)).isFalse();
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(limit - 8);
        assertThat(tracker.snapshotUsage().get("t1").memoryBytes()).isEqualTo(limit - 8);
        tracker.releaseMemory("t1", limit - 8);
        assertThat(tracker.getMemoryUsage("t1")).isZero();
        assertThat(tracker.tryAllocateMemory("t1", limit)).isTrue();
    }

    @Test
    void unlimitedMemoryStillRequiresRepresentableUsage() {
        assertThat(tracker.tryAllocateMemory("t1", Long.MAX_VALUE)).isTrue();
        assertThat(tracker.tryAllocateMemory("t1", 1)).isFalse();
        assertThat(tracker.getMemoryUsage("t1")).isEqualTo(Long.MAX_VALUE);
        tracker.releaseMemory("t1", Long.MAX_VALUE);
        assertThat(tracker.getMemoryUsage("t1")).isZero();
        assertThat(tracker.tryAllocateMemory("t1", 1)).isTrue();
    }

    @Test
    void concurrentLargeAllocationsCannotOverchargeOrWrap() throws Exception {
        long bytes = Long.MAX_VALUE / 4 + 1;
        var barrier = new java.util.concurrent.CyclicBarrier(4);
        var executor = java.util.concurrent.Executors.newFixedThreadPool(4,
                task -> new Thread(task, "tenant-memory-boundary-test"));
        try {
            var pending = java.util.stream.IntStream.range(0, 4).mapToObj(index -> executor.submit(() -> {
                barrier.await(10, java.util.concurrent.TimeUnit.SECONDS);
                return tracker.tryAllocateMemory("t1", bytes);
            })).toList();
            int admitted = 0;
            for (var task : pending) if (task.get(10, java.util.concurrent.TimeUnit.SECONDS)) admitted++;
            assertThat(admitted).isEqualTo(3);
            assertThat(tracker.getMemoryUsage("t1")).isEqualTo(bytes * 3);
            for (int index = 0; index < admitted; index++) tracker.releaseMemory("t1", bytes);
            assertThat(tracker.getMemoryUsage("t1")).isZero();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }
}
