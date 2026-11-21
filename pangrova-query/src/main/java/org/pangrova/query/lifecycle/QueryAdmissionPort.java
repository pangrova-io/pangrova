package org.pangrova.query.lifecycle;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.scheduler.api.admission.ClusterAdmissionController;

/** Query admission, throttling and cluster-slot obligations owned by the process assembly. */
public interface QueryAdmissionPort {

    void checkDegradation();

    void recordOutcome(boolean success);

    void checkCluster(String queryId, String tenantId, QueryStateMachine state);

    void checkThrottle(String queryId, QuerySession session);

    void releaseThrottle(String queryId);

    void setClusterController(ClusterAdmissionController controller);
}
