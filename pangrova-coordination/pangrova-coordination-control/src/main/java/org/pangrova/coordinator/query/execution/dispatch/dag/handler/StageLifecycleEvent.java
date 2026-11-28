package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable event carrying stage lifecycle context to handlers.
 * Passed to {@link StageLifecycleHandler} methods on each state transition.
 */
public record StageLifecycleEvent(
        Stage stage,
        StageDAG dag,
        String queryId,
        StageExecutionContext execution
) {

    public StageLifecycleEvent {
        checkNotNull(stage, "stage must not be null");
        checkNotNull(dag, "dag must not be null");
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(execution, "execution must not be null");
        if (!queryId.equals(execution.executionId())) {
            throw new IllegalArgumentException("Stage event execution identity differs from its control authority");
        }
    }
}
