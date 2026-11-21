package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.session.QuerySession;

import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.planner.throttle.ThrottlePermit;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.admission.ClusterAdmissionController;

/** Owns query admission reservations until dependent physical execution has drained. */
@ThreadSafe
final class QueryAdmission implements org.pangrova.query.lifecycle.QueryAdmissionPort {
    private final ResilienceRegistry resilience;
    private final QueryThrottlingService throttle;
    private final ConcurrentHashMap<String, ThrottlePermit> permits = new ConcurrentHashMap<>();
    private volatile ClusterAdmissionController cluster;

    QueryAdmission(ResilienceRegistry resilience, QueryThrottlingService throttle) {
        this.resilience = resilience;
        this.throttle = throttle;
    }

    @Override
    public void setClusterController(ClusterAdmissionController controller) { cluster = controller; }

    @Override
    public void checkDegradation() {
        if (resilience != null && !resilience.allowRequest("coordinator-query-admission")) {
            throw new QueryRejectedException("Query rejected: system overloaded — admission circuit breaker open");
        }
    }

    @Override
    public void recordOutcome(boolean success) {
        if (resilience == null) return;
        if (success) resilience.recordSuccess("coordinator-query-admission");
        else resilience.recordFailure("coordinator-query-admission", new IllegalStateException("query failed"));
    }

    @Override
    public void checkCluster(String queryId, String tenantId, QueryStateMachine state) {
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

    @Override
    public void checkThrottle(String queryId, QuerySession session) {
        if (throttle == null) return;
        String tenantId = session.tenantId() != null ? session.tenantId() : "default";
        String userId = session.userIdentity() != null ? session.userIdentity().principal() : "anonymous";
        var result = throttle.tryAcquire(tenantId, userId);
        if (!result.acquired()) throw new QueryRejectedException("Query throttled: " + result.reason());
        permits.put(queryId, result.permit());
    }

    @Override
    public void releaseThrottle(String queryId) {
        ThrottlePermit permit = permits.get(queryId);
        if (permit == null) return;
        throttle.release(permit);
        permits.remove(queryId, permit);
    }
}
