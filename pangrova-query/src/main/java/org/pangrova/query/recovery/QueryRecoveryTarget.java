package org.pangrova.query.recovery;

import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.lifecycle.QueryStateMachine;

/** Query-owner operations applied while replaying durable journal state. */
public interface QueryRecoveryTarget {

    /** Publishes a projection built from committed journal facts with issuance still unresolved. */
    void registerRecoveredStateMachine(String queryId, QueryStateMachine stateMachine);

    void onTaskFailed(String queryId, QueryFailure failure);
}
