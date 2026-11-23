/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.scheduler.admission;

import org.pangrova.testing.testkit.coordination.admission.InMemoryClusterConcurrencySlots;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks admission against an immutable usage sample at the {@link GlobalTenantUsageView} boundary.
 * Local slots and sample totals are component inputs, not independent-process cluster evidence.
 */
class TenantQuotaAdmissionControllerClusterTest {

    private static final ResourceQuota QUOTA = new ResourceQuota(
            10_737_418_240L,   // 10 GB maxMemory
            3,                 // maxConcurrentQueries
            5,                 // maxQueuedQueries
            3_600_000L,        // maxCpuTimePerQueryMs
            107_374_182_400L,  // maxScanBytesPerQuery
            10_000L,           // maxDailyQueryCount
            1_099_511_627_776L,// maxDailyScanBytes
            1_099_511_627_776L // maxStorageBytes
    );

    private InMemoryTenantStore tenantStore;

    @BeforeEach
    void setUp() {
        tenantStore = new InMemoryTenantStore();
        Tenant tenant = new Tenant("t1", "Test Tenant", TenantStatus.ACTIVE,
                QUOTA, List.of(), List.of(), null, Map.of(), Instant.now(), "admin");
        tenantStore.createTenant(tenant);
    }

    private TenantQuotaAdmissionController controllerWith(GlobalTenantUsageView view) {
        return new TenantQuotaAdmissionController(tenantStore, view);
    }

    @Test
    void rejectsWhenClusterMemoryPlusEstimateExceedsQuota() {
        // Cluster already using 9 GB for t1 (reported by other nodes' heartbeats).
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{9_000_000_000L, 0}));
        // 2 GB estimate alone is under the 10 GB limit, but 9 + 2 > 10 GB cluster-wide.
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1",
                OptionalLong.of(2_000_000_000L));

        AdmissionDecision decision = controllerWith(view).admit(req);

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("cluster memory");
    }

    @Test
    void admitsWhenClusterMemoryPlusEstimateWithinQuota() {
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{5_000_000_000L, 0}));
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1",
                OptionalLong.of(2_000_000_000L)); // 5 + 2 = 7 GB < 10 GB

        assertThat(controllerWith(view).admit(req).isAdmitted()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void wrappedClusterMemoryCannotAdmitOrLeakAReservedSlot(boolean useSharedSlots) {
        var tenant = tenantStore.getTenant("t1").orElseThrow();
        var quota = new ResourceQuota(Long.MAX_VALUE, 3, 5, 100, 100, 100, 100, 100);
        tenantStore.updateTenant(new Tenant(tenant.tenantId(), tenant.name(), tenant.status(), quota,
                tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy()));
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{Long.MAX_VALUE - 2, 0}));
        var slots = useSharedSlots ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(tenantStore, view, slots);

        var decision = controller.admit(new AdmissionRequest("t1", "default", "query",
                OptionalLong.of(10)));

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("cluster memory");
        assertThat(controller.admittedCount("t1")).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exactRepresentableClusterLimitCanBeAdmitted(boolean useSharedSlots) {
        var tenant = tenantStore.getTenant("t1").orElseThrow();
        var quota = new ResourceQuota(Long.MAX_VALUE, 3, 5, 100, 100, 100, 100, 100);
        tenantStore.updateTenant(new Tenant(tenant.tenantId(), tenant.name(), tenant.status(), quota,
                tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy()));
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{Long.MAX_VALUE - 10, 0}));
        var slots = useSharedSlots ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(tenantStore, view, slots);

        assertThat(controller.admit(new AdmissionRequest("t1", "default", "query",
                OptionalLong.of(10))).isAdmitted()).isTrue();
        assertThat(controller.admittedCount("t1")).isEqualTo(1);
        controller.release("query", "t1");
        assertThat(controller.admittedCount("t1")).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidNegativeClusterUsageFailsClosed(boolean useSharedSlots) {
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{-1, 0}));
        var slots = useSharedSlots ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(tenantStore, view, slots);

        var decision = controller.admit(new AdmissionRequest("t1", "default", "query", OptionalLong.of(1)));

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("negative cluster memory");
        assertThat(controller.admittedCount("t1")).isZero();
    }

    @Test
    void queuesWhenClusterConcurrencyAlreadySaturated() {
        // Cluster already at the concurrency limit (3) even though this coordinator
        // has admitted nothing locally.
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{0, 3}));
        AdmissionRequest req = AdmissionRequest.of("t1", "default", "q1");

        AdmissionDecision decision = controllerWith(view).admit(req);

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.QUEUE);
    }

    @Test
    void admittedCountReflectsClusterWideView() {
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{0, 5}));
        // SPI contract: admittedCount is cluster-wide, not coordinator-local.
        assertThat(controllerWith(view).admittedCount("t1")).isEqualTo(5);
    }

    @Test
    void nullViewFallsBackToLocalOnlyEnforcement() {
        TenantQuotaAdmissionController controller = controllerWith(null);
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "q1")).isAdmitted()).isTrue();
        assertThat(controller.admittedCount("t1")).isEqualTo(1);
    }

    @Test
    void sharedAdmissionAndQueueUseReservationOutcomeWithoutDiagnosticUsageReads() {
        var actual = new InMemoryClusterConcurrencySlots();
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var releases = new java.util.concurrent.atomic.AtomicInteger();
        var observed = new org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots() {
            @Override
            public boolean tryReserve(String tenant, String query, int limit) {
                return actual.tryReserve(tenant, query, limit);
            }

            @Override
            public void release(String tenant, String query) {
                releases.incrementAndGet();
                actual.release(tenant, query);
            }

            @Override
            public int active(String tenant) {
                reads.incrementAndGet();
                return actual.active(tenant);
            }
        };
        var controller = new TenantQuotaAdmissionController(tenantStore, null, observed);
        for (int index = 0; index < QUOTA.maxConcurrentQueries(); index++) {
            assertThat(controller.admit(AdmissionRequest.of("t1", "default", "query-" + index)).isAdmitted()).isTrue();
        }
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "queued")).verdict())
                .isEqualTo(AdmissionVerdict.QUEUE);
        assertThat(reads).hasValue(0);
        assertThat(releases).hasValue(0);
        controller.release("queued", "t1");
        assertThat(releases).hasValue(0);
        assertThat(controller.admittedCount("t1")).isEqualTo(QUOTA.maxConcurrentQueries());
        assertThat(reads).hasValue(1);
        controller.release("query-0", "t1");
        assertThat(releases).hasValue(1);
        assertThat(actual.active("t1")).isEqualTo(QUOTA.maxConcurrentQueries() - 1);
    }

    /** Immutable component input: tenant -> [memoryBytes, activeQueries]. */
    private static final class FixedGlobalUsageView implements GlobalTenantUsageView {
        private final Map<String, long[]> usage;

        FixedGlobalUsageView(Map<String, long[]> usage) {
            this.usage = usage;
        }

        @Override
        public long aggregatedMemoryBytes(String tenantId) {
            return usage.getOrDefault(tenantId, new long[]{0, 0})[0];
        }

        @Override
        public int aggregatedActiveQueries(String tenantId) {
            return (int) usage.getOrDefault(tenantId, new long[]{0, 0})[1];
        }

        @Override
        public Set<String> trackedTenants() {
            return usage.keySet();
        }
    }
}
