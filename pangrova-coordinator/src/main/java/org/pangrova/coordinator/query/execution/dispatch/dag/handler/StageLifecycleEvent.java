package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable event carrying stage lifecycle context to handlers.
 * Passed to {@link StageLifecycleHandler} methods on each state transition.
 */
public record StageLifecycleEvent(
        Stage stage,
        StageDAG dag,
        String queryId,
        CoordinatorSessionContext session
) {

    public StageLifecycleEvent {
        checkNotNull(stage, "stage must not be null");
        checkNotNull(dag, "dag must not be null");
        checkNotNull(queryId, "queryId must not be null");
    }
}
