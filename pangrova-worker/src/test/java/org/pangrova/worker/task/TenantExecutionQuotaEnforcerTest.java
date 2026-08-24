package org.pangrova.worker.task;

import org.pangrova.resource.tenant.TenantResourceQuota;
import org.pangrova.resource.tenant.TenantResourceTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates per-tenant quota enforcement: when a tenant exceeds its
 * concurrency or memory quota, new tasks are rejected.
 */
class TenantExecutionQuotaEnforcerTest {

    private TenantResourceTracker tracker;
    private TenantExecutionQuotaEnforcer enforcer;

    @BeforeEach
    void setUp() {
        tracker = new TenantResourceTracker();
        enforcer = new TenantExecutionQuotaEnforcer(tracker);
    }

    @Test
    void tenantExceedsConcurrencyQuota_newTasksRejected() {
        String tenantId = "tenant-A";
        tracker.setQuota(tenantId, new TenantResourceQuota(Long.MAX_VALUE, 2));

        // First two tasks admitted
        assertThat(enforcer.canAdmitTask(tenantId)).isTrue();
        enforcer.registerTask("task-1", tenantId);

        assertThat(enforcer.canAdmitTask(tenantId)).isTrue();
        enforcer.registerTask("task-2", tenantId);

        // Third task rejected — concurrency quota (2) exceeded
        assertThat(enforcer.canAdmitTask(tenantId)).isFalse();
    }

    @Test
    void tenantExceedsMemoryQuota_quotaExceededDetected() {
        String tenantId = "tenant-B";
        long maxMemory = 1000;
        tracker.setQuota(tenantId, new TenantResourceQuota(maxMemory, 100));

        // Allocate memory up to the limit
        assertThat(tracker.tryAllocateMemory(tenantId, 800)).isTrue();
        assertThat(tracker.tryAllocateMemory(tenantId, 300)).isFalse();

        enforcer.registerTask("task-mem-1", tenantId);

        // At 800/1000 — not over quota
        assertThat(enforcer.isQuotaExceeded("task-mem-1")).isFalse();

        // Reduce quota below current usage (simulates admin lowering quota)
        tracker.setQuota(tenantId, new TenantResourceQuota(500, 100));

        // Now 800 > 500 — over quota
        assertThat(enforcer.isQuotaExceeded("task-mem-1")).isTrue();
    }

    @Test
    void releaseTask_freesQuerySlot() {
        String tenantId = "tenant-C";
        tracker.setQuota(tenantId, new TenantResourceQuota(Long.MAX_VALUE, 1));

        // Admit one task
        assertThat(enforcer.canAdmitTask(tenantId)).isTrue();
        enforcer.registerTask("task-1", tenantId);

        // Second task rejected
        assertThat(enforcer.canAdmitTask(tenantId)).isFalse();

        // Release first task
        enforcer.releaseTask("task-1");

        // Now a new task can be admitted
        assertThat(enforcer.canAdmitTask(tenantId)).isTrue();
    }

    @Test
    void nullTenantId_alwaysAdmitted() {
        assertThat(enforcer.canAdmitTask(null)).isTrue();
        assertThat(enforcer.canAdmitTask("")).isTrue();
    }

    @Test
    void unregisteredTenant_unlimitedQuota() {
        String tenantId = "unknown-tenant";
        // No quota set — defaults to UNLIMITED
        assertThat(enforcer.canAdmitTask(tenantId)).isTrue();
        assertThat(enforcer.isQuotaExceeded("nonexistent-task")).isFalse();
    }

    @Test
    void perTenantAccounting_isolatesTenants() {
        String tenantA = "tenant-A";
        String tenantB = "tenant-B";
        tracker.setQuota(tenantA, new TenantResourceQuota(Long.MAX_VALUE, 1));
        tracker.setQuota(tenantB, new TenantResourceQuota(Long.MAX_VALUE, 1));

        // Tenant A uses its slot
        assertThat(enforcer.canAdmitTask(tenantA)).isTrue();
        enforcer.registerTask("task-A1", tenantA);

        // Tenant A is full
        assertThat(enforcer.canAdmitTask(tenantA)).isFalse();

        // Tenant B still has capacity — isolation works
        assertThat(enforcer.canAdmitTask(tenantB)).isTrue();
        enforcer.registerTask("task-B1", tenantB);
    }
}
