package org.pangrova.scheduler.core.admission;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.core.tenant.InMemoryTenantStore;
import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionVerdict;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks tenant admission with real component state; this does not execute SQL or Native tasks.
 */
class TenantQuotaAdmissionControllerTest {

    private static final ResourceQuota QUOTA = new ResourceQuota(
            10737418240L,   // 10 GB maxMemory
            3,              // maxConcurrentQueries
            5,              // maxQueuedQueries
            3600000L,       // maxCpuTimePerQueryMs
            107374182400L,  // maxScanBytesPerQuery
            10000L,         // maxDailyQueryCount
            1099511627776L, // maxDailyScanBytes
            1099511627776L  // maxStorageBytes
    );

    private InMemoryTenantStore tenantStore;
    private TenantQuotaAdmissionController controller;

    @BeforeEach
    void setUp() {
        tenantStore = new InMemoryTenantStore();
        controller = new TenantQuotaAdmissionController(tenantStore);

        Tenant tenant = new Tenant("t1", "Test Tenant", TenantStatus.ACTIVE,
                QUOTA, List.of(), List.of(), null, Map.of(), Instant.now(), "admin");
        tenantStore.createTenant(tenant);
    }

    @Test
    void admitsQueryWithinConcurrencyLimit() {
        AdmissionRequest req = AdmissionRequest.of("t1", "default", "q1");
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.isAdmitted()).isTrue();
        assertThat(controller.admittedCount("t1")).isEqualTo(1);
    }

    @Test
    void rejectsUnknownTenant() {
        AdmissionRequest req = AdmissionRequest.of("unknown", "default", "q1");
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("tenant not found");
    }

    @Test
    void rejectsSuspendedTenant() {
        Tenant suspended = new Tenant("t2", "Suspended", TenantStatus.SUSPENDED,
                QUOTA, List.of(), List.of(), null, Map.of(), Instant.now(), "admin");
        tenantStore.createTenant(suspended);

        AdmissionRequest req = AdmissionRequest.of("t2", "default", "q1");
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("SUSPENDED");
    }

    @Test
    void queuesWhenConcurrencyLimitReached() {
        // Admit 3 queries (maxConcurrentQueries = 3)
        controller.admit(AdmissionRequest.of("t1", "default", "q1"));
        controller.admit(AdmissionRequest.of("t1", "default", "q2"));
        controller.admit(AdmissionRequest.of("t1", "default", "q3"));
        assertThat(controller.admittedCount("t1")).isEqualTo(3);

        // 4th query should be queued
        AdmissionDecision decision = controller.admit(AdmissionRequest.of("t1", "default", "q4"));
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
    }

    @Test
    void releasedSlotAllowsNewAdmission() {
        controller.admit(AdmissionRequest.of("t1", "default", "q1"));
        controller.admit(AdmissionRequest.of("t1", "default", "q2"));
        controller.admit(AdmissionRequest.of("t1", "default", "q3"));
        assertThat(controller.admittedCount("t1")).isEqualTo(3);

        controller.release("q1", "t1");
        assertThat(controller.admittedCount("t1")).isEqualTo(2);

        // Now admission should succeed again
        AdmissionDecision decision = controller.admit(AdmissionRequest.of("t1", "default", "q4"));
        assertThat(decision.isAdmitted()).isTrue();
    }

    @Test
    void rejectsQueryExceedingMemoryEstimate() {
        long overLimit = QUOTA.maxMemoryBytes() + 1;
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1",
                OptionalLong.of(overLimit));
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("estimated memory");
    }

    @Test
    void admitsQueryWithMemoryEstimateWithinLimit() {
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1",
                OptionalLong.of(1024L * 1024 * 1024)); // 1 GB
        AdmissionDecision decision = controller.admit(req);
        assertThat(decision.isAdmitted()).isTrue();
    }

    @Test
    void admittedCountReturnsZeroForUnknownTenant() {
        assertThat(controller.admittedCount("nonexistent")).isEqualTo(0);
    }

    @Test
    void releaseIsIdempotentForUnknownQuery() {
        // Should not throw
        controller.release("unknown-query", "unknown-tenant");
    }
}
