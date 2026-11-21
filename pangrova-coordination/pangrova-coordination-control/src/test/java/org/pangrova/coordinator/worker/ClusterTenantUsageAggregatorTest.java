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
package org.pangrova.coordinator.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.pangrova.protocol.rest.worker.TenantUsageSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterTenantUsageAggregator} (P1-5 cross-node tenant
 * usage aggregation). Verifies the core oversell-detection scenario: the same
 * tenant reaching its per-node limit on multiple workers is summed into a global
 * total the coordinator can act on.
 */
class ClusterTenantUsageAggregatorTest {

    private static TenantUsageSnapshot usage(String tenantId, long memory, int queries) {
        return new TenantUsageSnapshot(tenantId, memory, queries);
    }

    @Test
    @DisplayName("sums per-tenant usage across all reporting workers")
    void aggregatesAcrossWorkers() {
        var aggregator = new ClusterTenantUsageAggregator();
        // Both workers independently admit for tenant t1 within their own node limit.
        aggregator.updateWorkerUsage("w1", Map.of("t1", usage("t1", 100, 5)));
        aggregator.updateWorkerUsage("w2", Map.of("t1", usage("t1", 200, 5)));

        assertThat(aggregator.aggregatedMemoryBytes("t1")).isEqualTo(300);
        assertThat(aggregator.aggregatedActiveQueries("t1")).isEqualTo(10);
        assertThat(aggregator.trackedTenants()).containsExactly("t1");
        assertThat(aggregator.reportingWorkerCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("latest report from a worker replaces its previous contribution")
    void latestReportReplacesPrevious() {
        var aggregator = new ClusterTenantUsageAggregator();
        aggregator.updateWorkerUsage("w1", Map.of("t1", usage("t1", 100, 5)));
        aggregator.updateWorkerUsage("w1", Map.of("t1", usage("t1", 50, 2)));

        assertThat(aggregator.aggregatedMemoryBytes("t1")).isEqualTo(50);
        assertThat(aggregator.aggregatedActiveQueries("t1")).isEqualTo(2);
        assertThat(aggregator.reportingWorkerCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("removing a worker drops its contribution (converges to live cluster)")
    void removingWorkerDropsContribution() {
        var aggregator = new ClusterTenantUsageAggregator();
        aggregator.updateWorkerUsage("w1", Map.of("t1", usage("t1", 100, 5)));
        aggregator.updateWorkerUsage("w2", Map.of("t1", usage("t1", 200, 5)));

        aggregator.removeWorker("w1");

        assertThat(aggregator.aggregatedMemoryBytes("t1")).isEqualTo(200);
        assertThat(aggregator.aggregatedActiveQueries("t1")).isEqualTo(5);
        assertThat(aggregator.reportingWorkerCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("an empty report removes the worker's contribution")
    void emptyReportRemovesWorker() {
        var aggregator = new ClusterTenantUsageAggregator();
        aggregator.updateWorkerUsage("w1", Map.of("t1", usage("t1", 100, 5)));
        aggregator.updateWorkerUsage("w1", Map.of());

        assertThat(aggregator.reportingWorkerCount()).isZero();
        assertThat(aggregator.aggregatedMemoryBytes("t1")).isZero();
    }

    @Test
    @DisplayName("unknown tenant reports zero and no tracked tenants")
    void unknownTenantIsZero() {
        var aggregator = new ClusterTenantUsageAggregator();
        assertThat(aggregator.aggregatedMemoryBytes("nope")).isZero();
        assertThat(aggregator.aggregatedActiveQueries("nope")).isZero();
        assertThat(aggregator.trackedTenants()).isEmpty();
    }

    @Test
    @DisplayName("tenants are isolated in the aggregate")
    void multipleTenantsIsolated() {
        var aggregator = new ClusterTenantUsageAggregator();
        aggregator.updateWorkerUsage("w1",
                Map.of("t1", usage("t1", 100, 1), "t2", usage("t2", 50, 2)));
        aggregator.updateWorkerUsage("w2", Map.of("t1", usage("t1", 100, 1)));

        assertThat(aggregator.aggregatedMemoryBytes("t1")).isEqualTo(200);
        assertThat(aggregator.aggregatedMemoryBytes("t2")).isEqualTo(50);
        assertThat(aggregator.aggregatedActiveQueries("t1")).isEqualTo(2);
        assertThat(aggregator.aggregatedActiveQueries("t2")).isEqualTo(2);
        assertThat(aggregator.trackedTenants()).containsExactlyInAnyOrder("t1", "t2");
    }
}
