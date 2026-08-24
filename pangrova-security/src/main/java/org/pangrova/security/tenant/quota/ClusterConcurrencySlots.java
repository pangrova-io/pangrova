package org.pangrova.security.tenant.quota;

/**
 * A-ADMISSION-01: an ATOMIC, cluster-wide concurrency-slot reservation for tenant admission.
 *
 * <p>The default {@link TenantQuotaAdmissionController} enforces the per-tenant concurrency limit
 * with a per-coordinator local counter plus a heartbeat-fed (lagged) {@code GlobalTenantUsageView}.
 * That read-then-admit is a TOCTOU: under a burst two coordinators can each observe
 * {@code active < limit} on the stale view and both admit, oversubscribing the tenant's limit.</p>
 *
 * <p>This abstraction replaces that with an atomic reserve-if-below-limit against a store shared by
 * all coordinators, so the sum of admitted queries across the cluster never exceeds the limit. A
 * single-JVM in-memory implementation is provided for single-node deployments; a JDBC-backed
 * implementation ({@code JdbcClusterConcurrencySlots}) provides the real cross-coordinator gate.</p>
 */
public interface ClusterConcurrencySlots {

    /**
     * Atomically reserves one slot for {@code tenantId} iff the current cluster-wide active count is
     * strictly below {@code limit}. A successful reservation must be balanced by exactly one
     * {@link #release}.
     *
     * @return {@code true} if a slot was reserved; {@code false} if the tenant is already at limit
     */
    boolean tryReserve(String tenantId, int limit);

    /** Releases one previously-reserved slot. Tolerates over-release (never drops below zero). */
    void release(String tenantId);

    /** The current cluster-wide active (reserved) count for the tenant. */
    int active(String tenantId);
}
