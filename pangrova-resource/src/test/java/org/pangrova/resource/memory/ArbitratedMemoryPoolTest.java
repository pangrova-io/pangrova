package org.pangrova.resource.memory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ArbitratedMemoryPool} verifying that the arbitrator
 * is the single source of truth for memory accounting.
 */
class ArbitratedMemoryPoolTest {

    private MemoryArbitrator arbitrator;

    @BeforeEach
    void setUp() {
        arbitrator = new MemoryArbitrator(10_000, 1_000); // 9000 allocatable
    }

    @Test
    void subPoolCreatesArbitratedPool() {
        ArbitratedMemoryPool pool = arbitrator.subPool("test-pool", null);

        assertThat(pool).isNotNull();
        assertThat(pool.getName()).isEqualTo("test-pool");
        assertThat(pool.getArbitrator()).isSameAs(arbitrator);
        assertThat(pool.getMaxBytes()).isEqualTo(9000); // allocatable budget
    }

    @Test
    void subPoolWithParentUsesParentMax() {
        RootMemoryPool parent = new RootMemoryPool("parent", 4096);
        ArbitratedMemoryPool pool = arbitrator.subPool("child", parent);

        assertThat(pool.getMaxBytes()).isEqualTo(4096);
    }

    @Test
    void reserveUpdatesArbitratorGlobalBudget() {
        ArbitratedMemoryPool pool = arbitrator.subPool("pool-a", null);

        long reserved = pool.tryReserve(1000);

        assertThat(reserved).isEqualTo(1000);
        assertThat(pool.getUsedBytes()).isEqualTo(1000);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(1000);
    }

    @Test
    void freeUpdatesArbitratorGlobalBudget() {
        ArbitratedMemoryPool pool = arbitrator.subPool("pool-a", null);
        pool.tryReserve(1000);

        pool.free(500);

        assertThat(pool.getUsedBytes()).isEqualTo(500);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(500);
    }

    @Test
    void reserveRespectsBothLocalAndGlobalLimits() {
        // Pool has 2000 max, arbitrator has 9000 allocatable
        RootMemoryPool parent = new RootMemoryPool("parent", 2000);
        ArbitratedMemoryPool pool = arbitrator.subPool("bounded", parent);

        long reserved = pool.tryReserve(3000);

        // Should be capped at pool's local max (2000)
        assertThat(reserved).isEqualTo(2000);
        assertThat(pool.getUsedBytes()).isEqualTo(2000);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(2000);
    }

    @Test
    void reserveRespectsGlobalBudgetExhaustion() {
        ArbitratedMemoryPool pool = arbitrator.subPool("big-pool", null);

        // Exhaust global budget via direct arbitrator allocation
        arbitrator.requestMemory("other-query", 8500);

        // Pool has 9000 max but only 500 available globally
        long reserved = pool.tryReserve(2000);

        assertThat(reserved).isEqualTo(500);
        assertThat(pool.getUsedBytes()).isEqualTo(500);
    }

    @Test
    void snapshotReflectsPoolAllocations() {
        ArbitratedMemoryPool poolA = arbitrator.subPool("pool-a", null);
        ArbitratedMemoryPool poolB = arbitrator.subPool("pool-b", null);

        poolA.tryReserve(1000);
        poolB.tryReserve(2000);

        Map<String, Long> snapshot = arbitrator.snapshot();

        assertThat(snapshot).containsEntry("pool-a", 1000L);
        assertThat(snapshot).containsEntry("pool-b", 2000L);
    }

    @Test
    void snapshotUpdatesAfterFree() {
        ArbitratedMemoryPool pool = arbitrator.subPool("pool-a", null);
        pool.tryReserve(1000);
        pool.free(400);

        Map<String, Long> snapshot = arbitrator.snapshot();

        assertThat(snapshot).containsEntry("pool-a", 600L);
    }

    @Test
    void poolAndArbitratorAlwaysAgree() {
        ArbitratedMemoryPool pool = arbitrator.subPool("consistent", null);

        pool.tryReserve(500);
        pool.tryReserve(300);
        pool.free(200);

        // Pool's local view and arbitrator's global view must match
        long poolUsed = pool.getUsedBytes();
        long arbitratorTracked = arbitrator.snapshot().getOrDefault("consistent", 0L);

        assertThat(poolUsed).isEqualTo(600);
        assertThat(arbitratorTracked).isEqualTo(600);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(600);
    }

    @Test
    void doubleReleaseThrows() {
        ArbitratedMemoryPool pool = arbitrator.subPool("pool-a", null);
        pool.tryReserve(100);

        assertThatThrownBy(() -> pool.free(200))
                .isInstanceOf(DoubleReleaseException.class);
    }

    @Test
    void multiplePoolsShareGlobalBudget() {
        ArbitratedMemoryPool poolA = arbitrator.subPool("pool-a", null);
        ArbitratedMemoryPool poolB = arbitrator.subPool("pool-b", null);

        poolA.tryReserve(5000);
        poolB.tryReserve(5000);

        // Global budget is 9000, so second pool gets only 4000
        assertThat(poolA.getUsedBytes()).isEqualTo(5000);
        assertThat(poolB.getUsedBytes()).isEqualTo(4000);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(9000);
    }

    @Test
    void clearAllocationsResetsPoolTracking() {
        ArbitratedMemoryPool pool = arbitrator.subPool("pool-a", null);
        pool.tryReserve(1000);

        arbitrator.clearAllocations();

        assertThat(arbitrator.getUsedMemory()).isZero();
        assertThat(arbitrator.snapshot()).isEmpty();
    }
}
