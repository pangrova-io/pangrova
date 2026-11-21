package org.pangrova.query.recovery;

import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.lifecycle.QueryStateMachine;

/** Query-owner operations applied while replaying durable journal state. */
public interface QueryRecoveryTarget {

    void markRecoveredIssuanceUnresolved(String queryId);

    void registerStateMachine(String queryId, QueryStateMachine stateMachine);

    void onTaskFailed(String queryId, QueryFailure failure);
}
