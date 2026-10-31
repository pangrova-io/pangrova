package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.planner.SqlCompiler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Per-execution context passed to statement factories.
 * Contains runtime state needed for statement execution.
 */
public record StatementExecutionContext(
        QueryStateMachine stateMachine,
        String queryId,
        CoordinatorSessionContext session,
        SqlCompiler compiler,
        CoordinatorResultService resultService) {

    public StatementExecutionContext {
        checkNotNull(queryId, "queryId");
    }
}
