package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.admission.QueryRejectedException;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.planner.throttle.ThrottlePermit;
import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.spi.resource.admission.AdmissionRequest;
import org.pangrova.spi.resource.admission.ClusterAdmissionController;

/** Owns query admission reservations until dependent physical execution has drained. */
@ThreadSafe
final class QueryAdmission {
    private final ResilienceRegistry resilience;
    private final QueryThrottlingService throttle;
    private final ConcurrentHashMap<String, ThrottlePermit> permits = new ConcurrentHashMap<>();
    private volatile ClusterAdmissionController cluster;

    QueryAdmission(ResilienceRegistry resilience, QueryThrottlingService throttle) {
        this.resilience = resilience;
        this.throttle = throttle;
    }

    static org.pangrova.coordinator.query.tenant.TenantQueryGuard tenantGuard(
            QueryManagerContext context, java.util.Collection<QueryStateMachine> queries) {
        if (context.tenantResolver() == null || context.quotaChecker() == null) return null;
        return new org.pangrova.coordinator.query.tenant.TenantQueryGuard(
                context.tenantResolver(), context.quotaChecker(),
                tenantId -> {
                    int concurrent = 0;
                    int queued = 0;
                    for (var state : queries) {
                        if (state.physicalCleanupApplied() || !tenantId.equals(state.getTenantId())) continue;
                        concurrent++;
                        if (state.getState() == QueryStateMachine.State.QUEUED) queued++;
                    }
                    return new org.pangrova.security.tenant.quota.TenantUsage(concurrent, queued, 0L, 0L);
                });
    }

    void setClusterController(ClusterAdmissionController controller) { cluster = controller; }

    void checkDegradation() {
        if (resilience != null && !resilience.allowRequest("coordinator-query-admission")) {
            throw new QueryRejectedException("Query rejected: system overloaded — admission circuit breaker open");
        }
    }

    void recordOutcome(boolean success) {
        if (resilience == null) return;
        if (success) resilience.recordSuccess("coordinator-query-admission");
        else resilience.recordFailure("coordinator-query-admission", new IllegalStateException("query failed"));
    }

    void checkCluster(String queryId, String tenantId, QueryStateMachine state) {
        ClusterAdmissionController controller = cluster;
        if (controller == null || tenantId == null || tenantId.isBlank()) return;
        // Retain the exact release obligation before admission can acquire a slot or queue entry.
        state.retainAdmissionRelease(() -> controller.release(queryId, tenantId));
        var decision = controller.admit(AdmissionRequest.of(tenantId, "default", queryId));
        if (!decision.isAdmitted()) {
            QueryRejectedException rejected = new QueryRejectedException(
                    "Cluster admission denied for tenant " + tenantId + ": " + decision.reason());
            try {
                state.releaseAdmission();
            } catch (RuntimeException | Error cleanup) {
                rejected.addSuppressed(cleanup);
            }
            throw rejected;
        }
    }

    void checkThrottle(String queryId, CoordinatorSessionContext session) {
        if (throttle == null) return;
        String tenantId = session.tenantId() != null ? session.tenantId() : "default";
        String userId = session.userIdentity() != null ? session.userIdentity().principal() : "anonymous";
        var result = throttle.tryAcquire(tenantId, userId);
        if (!result.acquired()) throw new QueryRejectedException("Query throttled: " + result.reason());
        permits.put(queryId, result.permit());
    }

    void releaseThrottle(String queryId) {
        ThrottlePermit permit = permits.get(queryId);
        if (permit == null) return;
        throttle.release(permit);
        permits.remove(queryId, permit);
    }
}
