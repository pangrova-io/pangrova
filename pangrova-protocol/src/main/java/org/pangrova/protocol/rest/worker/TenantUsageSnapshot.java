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
package org.pangrova.protocol.rest.worker;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable point-in-time usage sample for a single tenant on a single node.
 *
 * <p>Workers report these snapshots to the coordinator on each heartbeat so the
 * coordinator can aggregate a cluster-wide view of tenant resource consumption.
 * Without cross-node aggregation, the same tenant can independently reach its
 * per-node limit on every worker, allowing aggregate oversell.</p>
 *
 * @param tenantId      the tenant identifier (must not be null or blank)
 * @param memoryBytes   memory currently attributed to the tenant on the reporting node (clamped to &ge; 0)
 * @param activeQueries active queries currently attributed to the tenant on the reporting node (clamped to &ge; 0)
 */
@Experimental
public record TenantUsageSnapshot(String tenantId, long memoryBytes, int activeQueries) {

    public TenantUsageSnapshot {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId required");
        }
        if (memoryBytes < 0) {
            memoryBytes = 0;
        }
        if (activeQueries < 0) {
            activeQueries = 0;
        }
    }
}
