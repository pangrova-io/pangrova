package org.pangrova.worker.resource.memory;

import org.pangrova.worker.resource.tenant.TenantResourceQuota;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests per-tenant quota enforcement in {@link MemoryArbitrator}.
 */
class MemoryArbitratorTenantTest {

    private MemoryArbitrator arbitrator;
    private TenantResourceTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new TenantResourceTracker();
        arbitrator = new MemoryArbitrator(10_000, 1_000, tracker); // 9000 allocatable
    }

    @Test
    void tenantQuota_rejectsOverBudget() {
        tracker.setQuota("t1", new TenantResourceQuota(2000, -1));

        long a1 = arbitrator.requestMemory("q1", "t1", 1500);
        assertThat(a1).isEqualTo(1500);

        // Exceeds tenant quota of 2000
        long a2 = arbitrator.requestMemory("q2", "t1", 600);
        assertThat(a2).isEqualTo(0);

        // Within tenant quota
        long a3 = arbitrator.requestMemory("q3", "t1", 500);
        assertThat(a3).isEqualTo(500);
    }

    @Test
    void release_freesTenantBudget() {
        tracker.setQuota("t1", new TenantResourceQuota(1000, -1));

        arbitrator.requestMemory("q1", "t1", 1000);
        assertThat(arbitrator.requestMemory("q2", "t1", 1)).isEqualTo(0);

        arbitrator.release("q1", 500);
        assertThat(arbitrator.requestMemory("q3", "t1", 500)).isEqualTo(500);
    }

    @Test
    void noTracker_fallsBackToGlobal() {
        MemoryArbitrator noTenantArbitrator = new MemoryArbitrator(10_000, 1_000);

        long allocated = noTenantArbitrator.requestMemory("q1", "t1", 5000);
        assertThat(allocated).isEqualTo(5000);
    }

    @Test
    void nullTenantId_fallsBackToGlobal() {
        tracker.setQuota("t1", new TenantResourceQuota(100, -1));

        // null tenantId bypasses tenant check
        long allocated = arbitrator.requestMemory("q1", null, 5000);
        assertThat(allocated).isEqualTo(5000);
    }

    @Test
    void globalBudgetExhausted_tenantQuotaIrrelevant() {
        tracker.setQuota("t1", new TenantResourceQuota(100_000, -1));

        // Global allocatable is 9000
        long a1 = arbitrator.requestMemory("q1", "t1", 9000);
        assertThat(a1).isEqualTo(9000);

        // Tenant has budget but global is exhausted
        long a2 = arbitrator.requestMemory("q2", "t1", 1);
        assertThat(a2).isEqualTo(0);
    }

    @Test
    void differentTenants_independentBudgets() {
        tracker.setQuota("t1", new TenantResourceQuota(3000, -1));
        tracker.setQuota("t2", new TenantResourceQuota(3000, -1));

        assertThat(arbitrator.requestMemory("q1", "t1", 3000)).isEqualTo(3000);
        assertThat(arbitrator.requestMemory("q2", "t1", 1)).isEqualTo(0);

        // t2 is independent
        assertThat(arbitrator.requestMemory("q3", "t2", 3000)).isEqualTo(3000);
    }
}
