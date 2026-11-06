package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.query.core.task.DispatchResult;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.parser.api.ast.Statement;

/**
 * Functional interface for compiling a DML statement to a Substrait plan
 * and dispatching it to a native worker via the query pipeline.
 *
 * <p>Used by PlanExecutionFactory to delegate every relational DML
 * data-processing operation to native workers.</p>
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
                         QuerySession session,
                         Statement stmt);
}
