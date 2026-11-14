package org.pangrova.worker.task;

import org.pangrova.worker.resource.tenant.TenantResourceQuota;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
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
        assertThat(enforcer.tryReserveTask("task-1", tenantId)).isTrue();

        assertThat(enforcer.tryReserveTask("task-2", tenantId)).isTrue();

        // Third task rejected — concurrency quota (2) exceeded
        assertThat(enforcer.tryReserveTask("task-3", tenantId)).isFalse();
    }

    @Test
    void tenantExceedsMemoryQuota_quotaExceededDetected() {
        String tenantId = "tenant-B";
        long maxMemory = 1000;
        tracker.setQuota(tenantId, new TenantResourceQuota(maxMemory, 100));

        // Allocate memory up to the limit
        assertThat(tracker.tryAllocateMemory(tenantId, 800)).isTrue();
        assertThat(tracker.tryAllocateMemory(tenantId, 300)).isFalse();

        assertThat(enforcer.tryReserveTask("task-mem-1", tenantId)).isTrue();

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
        assertThat(enforcer.tryReserveTask("task-1", tenantId)).isTrue();

        // Second task rejected
        assertThat(enforcer.tryReserveTask("task-2", tenantId)).isFalse();

        // Release first task
        enforcer.releaseTask("task-1");

        // Now a new task can be admitted
        assertThat(enforcer.tryReserveTask("task-2", tenantId)).isTrue();
    }

    @Test
    void nullTenantId_alwaysAdmitted() {
        assertThat(enforcer.tryReserveTask("no-tenant", null)).isTrue();
        assertThat(enforcer.tryReserveTask("blank-tenant", "")).isTrue();
    }

    @Test
    void unregisteredTenant_unlimitedQuota() {
        String tenantId = "unknown-tenant";
        // No quota set — defaults to UNLIMITED
        assertThat(enforcer.tryReserveTask("unknown-tenant-task", tenantId)).isTrue();
        assertThat(enforcer.isQuotaExceeded("nonexistent-task")).isFalse();
    }

    @Test
    void perTenantAccounting_isolatesTenants() {
        String tenantA = "tenant-A";
        String tenantB = "tenant-B";
        tracker.setQuota(tenantA, new TenantResourceQuota(Long.MAX_VALUE, 1));
        tracker.setQuota(tenantB, new TenantResourceQuota(Long.MAX_VALUE, 1));

        // Tenant A uses its slot
        assertThat(enforcer.tryReserveTask("task-A1", tenantA)).isTrue();

        // Tenant A is full
        assertThat(enforcer.tryReserveTask("task-A2", tenantA)).isFalse();

        // Tenant B still has capacity — isolation works
        assertThat(enforcer.tryReserveTask("task-B1", tenantB)).isTrue();
    }

    @Test
    void duplicateReservationOwnsOnlyOneSlotAndReleaseIsIdempotent() {
        tracker.setQuota("tenant", new TenantResourceQuota(Long.MAX_VALUE, 2));
        assertThat(enforcer.tryReserveTask("same-task", "tenant")).isTrue();
        assertThat(enforcer.tryReserveTask("same-task", "tenant")).isTrue();
        assertThat(enforcer.getTenantActiveQueryCount("tenant")).isEqualTo(1);
        assertThat(enforcer.tryReserveTask("other-task", "tenant")).isTrue();
        enforcer.releaseTask("same-task");
        enforcer.releaseTask("same-task");
        assertThat(enforcer.getTenantActiveQueryCount("tenant")).isEqualTo(1);
    }

    @Test
    void taskIdentityCannotTransferItsQuotaToAnotherTenant() {
        assertThat(enforcer.tryReserveTask("same-task", "tenant-A")).isTrue();
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> enforcer.tryReserveTask("same-task", "tenant-B"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tenant cannot change");
        assertThat(enforcer.getTenantActiveQueryCount("tenant-A")).isEqualTo(1);
        assertThat(enforcer.getTenantActiveQueryCount("tenant-B")).isZero();
    }
}
