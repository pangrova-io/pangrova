package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.query.dispatch.StatementCompletionListener;

import java.util.List;

import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.authorization.AccessControl;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.query.dispatch.StatementExecutionFactory;

/**
 * Immutable dependency bundle for StatementDispatcher construction.
 * Groups constructor parameters into a single cohesive object.
 */
public record StatementDispatcherContext(
        SqlCompiler compiler,
        CoordinatorResultService resultService,
        List<StatementExecutionFactory<?>> factories,
        List<StatementCompletionListener> listeners,
        AccessControl accessControl,
        DdlLeadershipGuard ddlGuard) {

    public StatementDispatcherContext {
        checkNotNull(factories, "factories");
        factories = List.copyOf(factories);
        listeners = listeners != null ? List.copyOf(listeners) : List.of();
        // compiler is nullable — late-bound via StatementDispatcher.updateCompiler()
        // accessControl is nullable for anonymous/test contexts
        // ddlGuard is nullable — when null, DDL executes on any coordinator
    }

    /** Creates a context without a DDL leadership guard. */
    public StatementDispatcherContext(
            SqlCompiler compiler,
            CoordinatorResultService resultService,
            List<StatementExecutionFactory<?>> factories,
            List<StatementCompletionListener> listeners,
            AccessControl accessControl) {
        this(compiler, resultService, factories, listeners, accessControl, null);
    }
}
