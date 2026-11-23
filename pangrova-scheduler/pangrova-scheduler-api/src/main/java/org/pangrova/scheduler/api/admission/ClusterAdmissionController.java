package org.pangrova.scheduler.api.admission;

import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for cluster-level query admission control.
 *
 * <p>An admission controller decides whether an inbound query may proceed to
 * scheduling or should be rejected. The decision is
 * based on per-tenant quotas, resource-group concurrency limits, and
 * optionally CBO-estimated memory reservations.</p>
 *
 * <p>This SPI addresses several gaps identified in G-RES-04:</p>
 * <ul>
 *   <li><b>Cluster-level enforcement</b> -- existing {@code QuotaChecker}
 *       operates only on a single coordinator's local counters; this SPI
 *       mandates a cluster-wide authoritative view (backed by heartbeat-fed
 *       per-tenant accounting or partitioned quotas).</li>
 *   <li><b>Atomic admission</b> -- the existing check-then-act pattern has a
 *       TOCTOU race; implementations must use CAS or lock-guarded reservation
 *       to prevent over-admission.</li>
 *   <li><b>Memory dimension</b> -- existing admission checks only concurrency;
 *       this SPI accepts a CBO memory estimate so that the fit-filter is
 *       effective.</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe; the admission gate is on the
 * coordinator's query submission hot path.</p>
 *
 * @see AdmissionRequest
 * @see AdmissionDecision
 */
@Experimental(reason = "G-RES-04: cluster admission control SPI "
        + "-- existing QuotaChecker lacks cluster-level enforcement and memory dimension")
public interface ClusterAdmissionController {

    /**
     * Evaluates whether the given query should be admitted or rejected.
     *
     * <p>Admission must be atomic: concurrent callers must not cause the system
     * to exceed the configured concurrency or memory limits. A returned REJECT
     * leaves this request without a new reservation. An exception can leave an
     * unresolved reservation that requires reconciliation.</p>
     *
     * @param request the admission request context (tenant, resource group, query, memory estimate)
     * @return the admission decision (never null)
     */
    AdmissionDecision admit(AdmissionRequest request);

    /**
     * Releases the admission reservation for a completed or cancelled query.
     *
     * <p>Must be called exactly once per admitted query to return capacity to
     * the pool. Implementations must tolerate a release for a query that was
     * never admitted (e.g. due to crash recovery) -- in that case the call is a
     * no-op.</p>
     *
     * @param queryId  the query whose reservation to release
     * @param tenantId the tenant who owns the query
     */
    void release(String queryId, String tenantId);

    /**
     * Returns the current number of admitted (in-flight) queries for the given tenant.
     *
     * <p>This is a cluster-wide count, not a coordinator-local count.</p>
     *
     * @param tenantId the tenant to query
     * @return the number of admitted queries for this tenant (non-negative)
     */
    int admittedCount(String tenantId);
}
