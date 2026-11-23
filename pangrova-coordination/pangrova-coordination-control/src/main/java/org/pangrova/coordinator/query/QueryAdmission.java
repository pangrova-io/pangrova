package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.session.QuerySession;

import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.planner.throttle.ThrottlePermit;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.scheduler.api.enums.AdmissionVerdict;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.security.exception.TenantQuotaExceededException;
import org.pangrova.scheduler.api.admission.ClusterAdmissionController;

/** Owns query admission reservations until dependent physical execution has drained. */
@ThreadSafe
final class QueryAdmission implements org.pangrova.query.lifecycle.QueryAdmissionPort {
    private static final java.util.Set<String> IGNORED_ERROR_CODES = java.util.stream.Stream.of(
            PangrovaErrorCode.TENANT_QUOTA_EXCEEDED, PangrovaErrorCode.TENANT_NOT_FOUND,
            PangrovaErrorCode.COORDINATOR_QUERY_REJECTED, PangrovaErrorCode.ACCESS_DENIED,
            PangrovaErrorCode.PARSER_SYNTAX_ERROR, PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
            PangrovaErrorCode.PARSER_INPUT_TOO_LARGE, PangrovaErrorCode.PLANNER_VALIDATION_FAILURE)
            .map(PangrovaErrorCode::getCode).collect(java.util.stream.Collectors.toUnmodifiableSet());

    private final CircuitBreaker breaker;
    private final QueryThrottlingService throttle;
    private final ConcurrentHashMap<String, ThrottlePermit> permits = new ConcurrentHashMap<>();
    private volatile ClusterAdmissionController cluster;

    QueryAdmission(ResilienceRegistry resilience, QueryThrottlingService throttle) {
        this.breaker = resilience == null ? null : resilience.getCircuitBreaker("coordinator-query-admission");
        this.throttle = throttle;
    }

    @Override
    public void setClusterController(ClusterAdmissionController controller) { cluster = controller; }

    @Override
    public boolean acquireHealthProbe() {
        if (breaker == null) return false;
        if (!breaker.tryAcquirePermission()) {
            throw new QueryRejectedException("Query rejected: system overloaded — admission circuit breaker open");
        }
        return true;
    }

    @Override
    public void recordOutcome(QueryState state, QueryFailure failure) {
        if (state == QueryState.FINISHED) {
            breaker.onSuccess(0, java.util.concurrent.TimeUnit.NANOSECONDS);
        } else if (state == QueryState.CANCELLED
                || failure != null && failure.errorCode() != null
                && IGNORED_ERROR_CODES.contains(failure.errorCode())) {
            breaker.releasePermission();
        } else {
            breaker.onError(0, java.util.concurrent.TimeUnit.NANOSECONDS,
                    new IllegalStateException(failure == null ? "Query failed" : failure.message()));
        }
    }

    @Override
    public void checkCluster(String queryId, ResolvedTenantContext tenant, QueryStateMachine state) {
        ClusterAdmissionController controller = cluster;
        if (controller == null) return;
        if (tenant == null) {
            throw new IllegalStateException("Cluster admission requires a resolved tenant");
        }
        String tenantId = tenant.tenantId();
        var quota = tenant.tenant().quota();
        var limits = new TenantLimits(quota.maxMemoryBytes(), quota.maxConcurrentQueries(), quota.maxQueuedQueries());
        // Retain the exact release obligation before admission can acquire a slot or queue entry.
        state.retainAdmissionRelease(() -> controller.release(queryId, tenantId));
        var decision = controller.admit(AdmissionRequest.of(tenantId, "default", queryId, limits));
        if (!decision.isAdmitted()) {
            var rejected = new TenantQuotaExceededException(
                    "Cluster admission denied for tenant " + tenantId + ": " + decision.reason());
            if (decision.verdict() == AdmissionVerdict.REJECT) {
                state.discardUnacquiredAdmission();
            } else {
                try {
                    state.releaseAdmission();
                } catch (RuntimeException | Error cleanup) {
                    rejected.addSuppressed(cleanup);
                }
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
