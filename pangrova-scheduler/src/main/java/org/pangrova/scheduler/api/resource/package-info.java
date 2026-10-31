/**
 * Cluster-level query admission contract: request/decision value types and the
 * controller port consumed by the coordinator's submission path.
 *
 * <p>{@link org.pangrova.scheduler.api.resource.ClusterAdmissionController} decides
 * whether an inbound query proceeds to scheduling, is queued, or is rejected. The
 * decision considers per-tenant quotas, resource-group concurrency and queue limits,
 * and optionally CBO-estimated memory. Implementations live in
 * {@code org.pangrova.scheduler.core.admission}.</p>
 */
package org.pangrova.scheduler.api.resource;
