package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.scheduler.api.admission.ClusterAdmissionController;

/** Query admission, throttling and cluster-slot obligations owned by the process assembly. */
public interface QueryAdmissionPort {

    /** Acquires the local health probe if one is configured. */
    boolean acquireHealthProbe();

    /** Completes a previously acquired local health probe with the exact terminal outcome. */
    void recordOutcome(QueryState state, QueryFailure failure);

    void checkCluster(String queryId, ResolvedTenantContext tenant, QueryStateMachine state);

    void checkThrottle(String queryId, QuerySession session);

    void releaseThrottle(String queryId);

    void setClusterController(ClusterAdmissionController controller);
}
