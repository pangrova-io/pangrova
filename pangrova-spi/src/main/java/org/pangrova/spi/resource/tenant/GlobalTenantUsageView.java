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
package org.pangrova.spi.resource.tenant;

import java.util.Set;

import org.pangrova.spi.annotation.Experimental;

/**
 * Read-only cluster-wide view of tenant resource usage aggregated from all
 * worker heartbeats.
 *
 * <p>Local per-node trackers only know what happened on their own node; this
 * view sums per-tenant usage across every reporting worker so admission control
 * can reason about the whole cluster rather than a single node. Consulted by the
 * coordinator admission gate to prevent aggregate oversell (the same tenant
 * reaching its limit independently on each worker).</p>
 *
 * <p>Implementations must be safe for concurrent reads while worker reports are
 * being ingested.</p>
 */
@Experimental
public interface GlobalTenantUsageView {

    /**
     * Returns the cluster-wide memory in bytes currently attributed to the tenant,
     * summed across all reporting workers. Returns 0 for an unknown tenant.
     */
    long aggregatedMemoryBytes(String tenantId);

    /**
     * Returns the cluster-wide active query count for the tenant, summed across
     * all reporting workers. Returns 0 for an unknown tenant.
     */
    int aggregatedActiveQueries(String tenantId);

    /**
     * Returns the set of tenants with any reported usage across the cluster.
     */
    Set<String> trackedTenants();
}
