package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.parser.ast.Statement;

/**
 * Functional interface for compiling a DML statement to a Substrait plan
 * and dispatching it to a native worker via the query pipeline.
 *
 * <p>Used by PlanExecutionFactory and DmlExecutor to delegate
 * data-processing operations to native workers instead of executing
 * them on the coordinator JVM.</p>
 */
@FunctionalInterface
public interface NativeDmlCompileFunction {

    /**
     * Compiles the statement to a Substrait plan and dispatches to a worker.
     *
     * @param stateMachine query state machine for lifecycle tracking
     * @param session      coordinator session context
     * @param stmt         the DML statement to compile and dispatch
     * @return dispatch result with worker task information
     */
    DispatchResult apply(QueryStateMachine stateMachine,
                         CoordinatorSessionContext session,
                         Statement stmt);
}
