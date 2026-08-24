package org.pangrova.resource.memory;

/**
 * Coordinates memory accounting across Java control plane and Native execution domain.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Java→Native: push tenant quota changes to native side via proto message</li>
 *   <li>Native→Java: receive periodic {@link NativeMemoryEvent} heartbeats and
 *       spill/shrink/oom notifications</li>
 *   <li>Provide combined (Java + Native) usage per tenant for arbitration decisions</li>
 * </ul>
 *
 * <p>Failure mode: if native heartbeat is stale (no event received within
 * the configured timeout), the coordinator degrades to Java-only view and
 * increments the {@code native_stat_stale_total} metric.
 *
 * <p>Consistency model: async sync with worst-case window = heartbeat interval + RPC RTT.
 */
public interface CrossDomainMemoryCoordinator {

    /**
     * Publishes a tenant quota change to the native execution domain.
     *
     * <p>Called when the Java control plane adjusts a tenant's memory budget.
     * The implementation pushes a setTenantQuota proto message to native side.
     *
     * @param tenantId tenant identifier (must not be null or blank)
     * @param bytes    new quota in bytes (must be positive)
     */
    void publishQuotaChange(String tenantId, long bytes);

    /**
     * Receives a memory event from the native execution domain.
     *
     * <p>Called when native reports a periodic stat heartbeat or a
     * spill/shrink/oom event. Updates the internal native usage ledger.
     *
     * @param event native memory event (must not be null)
     */
    void onNativeEvent(NativeMemoryEvent event);

    /**
     * Returns the combined memory usage for a tenant across Java and Native domains.
     *
     * <p>If native stats are stale (heartbeat timeout exceeded), returns
     * Java-only usage and logs a degraded-mode warning.
     *
     * @param tenantId tenant identifier (must not be null or blank)
     * @return combined usage in bytes (Java arbitrator + latest native stat)
     */
    long combinedUsage(String tenantId);

    /**
     * Checks whether native stats for the given tenant are considered stale.
     *
     * @param tenantId tenant identifier
     * @return true if no native heartbeat received within the configured timeout
     */
    boolean isNativeStatStale(String tenantId);
}
