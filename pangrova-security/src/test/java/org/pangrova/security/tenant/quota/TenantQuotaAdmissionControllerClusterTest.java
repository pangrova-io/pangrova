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
package org.pangrova.security.tenant.quota;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.store.InMemoryTenantStore;
import org.pangrova.spi.resource.admission.AdmissionDecision;
import org.pangrova.spi.resource.admission.AdmissionRequest;
import org.pangrova.spi.resource.admission.AdmissionVerdict;
import org.pangrova.spi.resource.tenant.GlobalTenantUsageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates P1-5 phase 2: {@link TenantQuotaAdmissionController} enforces
 * cluster-wide tenant limits using a heartbeat-fed {@link GlobalTenantUsageView},
 * not just this coordinator's local counters. Uses a real in-memory view
 * implementation (no mocks) per project testing rules.
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
        return new TenantQuotaAdmissionController(tenantStore, new QuotaChecker(), view);
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

    /** Real in-memory {@link GlobalTenantUsageView}: tenant -> [memoryBytes, activeQueries]. */
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
