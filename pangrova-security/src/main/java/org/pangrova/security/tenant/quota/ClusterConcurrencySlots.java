package org.pangrova.security.tenant.quota;

/**
 * Atomic live query reservations. Tenant observations cannot substitute for this authority.
 * Query identifiers must belong to the issuing query authority and never be reused after its
 * physical lifetime closes. A JDBC reservation survives this object's/process's replacement;
 * the in-memory implementation supplies process-local semantics only.
 */
public interface ClusterConcurrencySlots {

    /**
     * Reserves once for an exact query/tenant pair, or acknowledges its existing live reservation.
     * New reservations must not exceed the limit. Backend errors must surface, not mean saturation.
     *
     * @param tenantId authenticated tenant
     * @param queryId globally unique live query identity, never reused after physical completion
     * @param limit maximum live reservations for this tenant
     * @return {@code true} if a slot was reserved; {@code false} if the tenant is already at limit
     */
    boolean tryReserve(String tenantId, String queryId, int limit);

    /**
     * Returns only the exact live query reservation; unknown and duplicate releases are no-ops.
     * Backend failures retain the reservation and must surface so its owner can retry.
     * @param tenantId authenticated tenant
     * @param queryId physically drained query whose reservation is being returned
     */
    void release(String tenantId, String queryId);

    /** The current cluster-wide active (reserved) count for the tenant. */
    int active(String tenantId);
}
