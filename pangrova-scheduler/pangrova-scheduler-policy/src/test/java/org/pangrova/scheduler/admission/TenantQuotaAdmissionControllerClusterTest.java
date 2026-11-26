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

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks admission against an immutable usage sample at the {@link GlobalTenantUsageView} boundary.
 * Local slots and sample totals are component inputs, not independent-process cluster evidence.
 */
class TenantQuotaAdmissionControllerClusterTest {

    private static final TenantLimits QUOTA = new TenantLimits(10L << 30, 3);

    private TenantQuotaAdmissionController controllerWith(GlobalTenantUsageView view) {
        return new TenantQuotaAdmissionController(view, null);
    }

    @Test
    void rejectsWhenClusterMemoryPlusEstimateExceedsQuota() {
        // Cluster already using 9 GB for t1 (reported by other nodes' heartbeats).
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{9_000_000_000L, 0}));
        // 2 GB estimate alone is under the 10 GB limit, but 9 + 2 > 10 GB cluster-wide.
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1", QUOTA,
                OptionalLong.of(2_000_000_000L));

        AdmissionDecision decision = controllerWith(view).admit(req);

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("cluster memory");
    }

    @Test
    void admitsWhenClusterMemoryPlusEstimateWithinQuota() {
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{5_000_000_000L, 0}));
        AdmissionRequest req = new AdmissionRequest("t1", "default", "q1", QUOTA,
                OptionalLong.of(2_000_000_000L)); // 5 + 2 = 7 GB < 10 GB

        assertThat(controllerWith(view).admit(req).isAdmitted()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void wrappedClusterMemoryCannotAdmitOrLeakAReservedSlot(boolean useSharedSlots) {
        var quota = new TenantLimits(Long.MAX_VALUE, 3);
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{Long.MAX_VALUE - 2, 0}));
        var slots = useSharedSlots ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(view, slots);

        var decision = controller.admit(new AdmissionRequest("t1", "default", "query", quota,
                OptionalLong.of(10)));

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("cluster memory");
        assertThat(controller.admittedCount("t1")).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exactRepresentableClusterLimitCanBeAdmitted(boolean useSharedSlots) {
        var quota = new TenantLimits(Long.MAX_VALUE, 3);
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{Long.MAX_VALUE - 10, 0}));
        var slots = useSharedSlots ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(view, slots);

        assertThat(controller.admit(new AdmissionRequest("t1", "default", "query", quota,
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
        var controller = new TenantQuotaAdmissionController(view, slots);

        var decision = controller.admit(new AdmissionRequest("t1", "default", "query", QUOTA, OptionalLong.of(1)));

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
        assertThat(decision.reason()).contains("negative cluster memory");
        assertThat(controller.admittedCount("t1")).isZero();
    }

    @Test
    void rejectsWhenClusterConcurrencyAlreadySaturated() {
        // Cluster already at the concurrency limit (3) even though this coordinator
        // has admitted nothing locally.
        var view = new FixedGlobalUsageView(Map.of("t1", new long[]{0, 3}));
        AdmissionRequest req = AdmissionRequest.of("t1", "default", "q1", QUOTA);

        AdmissionDecision decision = controllerWith(view).admit(req);

        assertThat(decision.verdict()).isEqualTo(AdmissionVerdict.REJECT);
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
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "q1", QUOTA)).isAdmitted()).isTrue();
        assertThat(controller.admittedCount("t1")).isEqualTo(1);
    }

    @Test
    void sharedAdmissionUsesReservationOutcomeWithoutDiagnosticUsageReads() {
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
        var controller = new TenantQuotaAdmissionController(null, observed);
        for (int index = 0; index < QUOTA.maxConcurrentQueries(); index++) {
            var request = AdmissionRequest.of("t1", "default", "query-" + index, QUOTA);
            assertThat(controller.admit(request).isAdmitted()).isTrue();
        }
        assertThat(controller.admit(AdmissionRequest.of("t1", "default", "over-capacity", QUOTA)).verdict())
                .isEqualTo(AdmissionVerdict.REJECT);
        assertThat(reads).hasValue(0);
        assertThat(releases).hasValue(0);
        assertThat(controller.admittedCount("t1")).isEqualTo(QUOTA.maxConcurrentQueries());
        assertThat(reads).hasValue(1);
        controller.release("query-0", "t1");
        assertThat(releases).hasValue(1);
        assertThat(actual.active("t1")).isEqualTo(QUOTA.maxConcurrentQueries() - 1);
    }

    @Test
    void rejectedMemoryEstimateNeverAcquiresOrReturnsASharedSlot() {
        var slots = new org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots() {
            @Override public boolean tryReserve(String tenant, String query, int limit) {
                throw new AssertionError("A rejected estimate must not reserve capacity");
            }
            @Override public void release(String tenant, String query) {
                throw new AssertionError("A rejected estimate must not return capacity");
            }
            @Override public int active(String tenant) {
                throw new AssertionError("A rejected estimate must not read usage");
            }
        };
        var controller = new TenantQuotaAdmissionController(null, slots);
        var request = new AdmissionRequest("t1", "default", "too-large", QUOTA,
                OptionalLong.of(QUOTA.maxMemoryBytes() + 1));

        assertThat(controller.admit(request).verdict()).isEqualTo(AdmissionVerdict.REJECT);
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
