package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.dispatch.StatementCompletionEvent;
import org.pangrova.query.dispatch.StatementCompletionListener;

import org.pangrova.execution.task.DispatchResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.pangrova.execution.api.ownership.ExecutionAnchor;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.observability.logging.LogContext;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;

/**
 * Pure dispatcher for DDL/DML/utility statements.
 * Delegates execution to registered factories and fires completion events.
 */
public final class StatementDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(StatementDispatcher.class);

    private final Map<Class<? extends Statement>, StatementExecutionFactory<?>> registry;
    private final List<StatementCompletionListener> listeners;
    private final CoordinatorResultService resultService;
    private final AccessControl accessControl;
    private final DdlLeadershipGuard ddlGuard;
    private final Predicate<ExecutionAnchor> reserveCoordinatorStatement;

    /** Creates a statement dispatcher from its execution dependencies. */
    public StatementDispatcher(StatementDispatcherContext deps) {
        this(deps, null);
    }

    /** Installs the exact durable work boundary when the Coordinator has a shared journal. */
    public StatementDispatcher(StatementDispatcherContext deps,
            Predicate<ExecutionAnchor> reserveCoordinatorStatement) {
        this.resultService = deps.resultService();
        this.reserveCoordinatorStatement = reserveCoordinatorStatement;
        this.accessControl = deps.accessControl();
        this.ddlGuard = deps.ddlGuard();
        this.registry = new ConcurrentHashMap<>();
        this.listeners = new CopyOnWriteArrayList<>(deps.listeners());
        deps.factories().forEach(this::registerAll);
    }

    /**
     * Attempts to dispatch the AST selected by the query phase.
     * Returns null if the statement belongs to relational planning.
     */
    public CompletionStage<DispatchResult> tryDispatch(
            QueryStateMachine sm, Statement ast,
            String queryId, QuerySession session) {
        if (ast == null || !isRoutable(ast)) {
            return null;
        }
        // DDL leader routing guard: when enabled, rejects DDL on non-leader nodes
        // with HTTP 307 redirect. When disabled (default), DDL runs on any coordinator.
        if (ddlGuard != null) {
            ddlGuard.requireLeaderForDdl(ast);
        }
        StatementExecutionFactory<?> factory = registry.get(ast.getClass());
        if (factory == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No factory registered for: " + ast.getClass().getSimpleName());
        }
        checkPermission(ast, session);
        var context = new StatementExecutionContext(sm, queryId, session);
        return startUnchecked(factory, ast, context);
    }

    private DispatchResult complete(Statement statement, StatementExecutionContext context, StatementResult result,
            boolean coordinatorOwned) {
        if (!coordinatorOwned && (!result.rows().isEmpty() || !result.columnNames().isEmpty())) {
            throw new IllegalStateException("Native statement route cannot publish a Coordinator result");
        }
        var trace = context.session().traceContext();
        var previousLogContext = LogContext.snapshot();
        try (var scope = trace.makeCurrent()) {
            LogContext.set(trace.traceId(), trace.spanId());
            LogContext.setQueryId(context.queryId());
            fireCompletionEvent(context.queryId(), statement, result, context.session());
            return toDispatchResult(result, context.queryId(), context.stateMachine());
        } finally {
            LogContext.restore(previousLogContext);
        }
    }

    /**
     * Executes a pre-parsed statement directly, bypassing SQL parsing.
     * Used by coordinator tests that already have a parsed AST.
     * QueryStateMachine is nullable for pure metadata operations.
     */
    public DispatchResult executeDirect(
            Statement ast, QueryStateMachine sm,
            String queryId, QuerySession session) {
        if (!isRoutable(ast)) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Statement is not routable: " + ast.getClass().getSimpleName());
        }
        StatementExecutionFactory<?> factory = registry.get(ast.getClass());
        if (factory == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No factory registered for: " + ast.getClass().getSimpleName());
        }
        PermissionChecker.check(ast, session, accessControl);
        var context = new StatementExecutionContext(sm, queryId, session);
        StatementResult result = executeUnchecked(factory, ast, context);
        fireCompletionEvent(queryId, ast, result, session);
        return toDispatchResult(result, queryId, sm);
    }

    /** Registers additional factories after construction for dynamic handlers. */
    public void registerFactory(StatementExecutionFactory<?> factory) {
        registerAll(factory);
    }

    /**
     * Enforces table-level access control before factory execution.
     * Delegates to PermissionChecker for consistent permission enforcement.
     */
    private void checkPermission(Statement stmt, QuerySession session) {
        PermissionChecker.check(stmt, session, accessControl);
    }

    /**
     * Returns true if a registered factory can handle this statement type.
     * Registry-driven: a statement is routable iff a factory has been registered
     * for its concrete class. No delegation to StatementClassifier.isRoutable().
     */
    public boolean isRoutable(Statement stmt) {
        return registry.containsKey(stmt.getClass());
    }

    @SuppressWarnings("unchecked")
    private <T extends Statement> StatementResult executeUnchecked(
            StatementExecutionFactory<T> factory, Statement stmt,
            StatementExecutionContext context) {
        return factory.execute((T) stmt, context);
    }

    @SuppressWarnings("unchecked")
    private <T extends Statement> CompletionStage<DispatchResult> startUnchecked(
            StatementExecutionFactory<T> factory, Statement stmt, StatementExecutionContext context) {
        T statement = (T) stmt;
        boolean coordinatorOwned = factory.usesCoordinatorStatementAuthority(statement);
        if (reserveCoordinatorStatement != null && coordinatorOwned) {
            var anchor = context.stateMachine().journalGrant();
            if (anchor == null || !reserveCoordinatorStatement.test(anchor)) {
                throw new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED,
                        "Coordinator statement grant no longer permits execution: " + context.queryId());
            }
        }
        return factory.start(statement, context)
                .thenApply(result -> complete(statement, context, result, coordinatorOwned));
    }

    private void fireCompletionEvent(
            String queryId, Statement stmt,
            StatementResult result, QuerySession session) {
        var event = new StatementCompletionEvent(queryId, stmt, result, session);
        for (StatementCompletionListener listener : listeners) {
            try {
                listener.onCompleted(event);
            } catch (Exception e) {
                LOG.warn("event=completion_listener_failed listener={} queryId={} error={}",
                        listener.getClass().getSimpleName(), queryId, e.getMessage());
            }
        }
    }

    private DispatchResult toDispatchResult(
            StatementResult result, String queryId, QueryStateMachine sm) {
        if (sm == null) {
            return null;
        }
        if (resultService != null && (!result.rows().isEmpty() || !result.columnNames().isEmpty())) {
            if (sm.isTerminal()) {
                throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                        "Cannot publish a control result for a terminal query: " + sm.getState());
            }
            var control = resultService.dispatchControlPlaneRows(
                    queryId, result.rows(), result.columnNames(), result.columnTypes());
            // The result batch and endpoint must be visible before FINISHED:
            // Native CTAS may already have completed, while an outer DDL still
            // needs to publish its metadata head and acknowledge the statement.
            try {
                sm.setResultEndpoint(control.resultEndpoint());
                sm.toFinisher().run();
                return control;
            } catch (RuntimeException | Error failure) {
                org.pangrova.common.lifecycle.ResourceCloser.closeAll(failure,
                        () -> resultService.discardUnopened(queryId));
                throw failure;
            }
        }
        sm.markNoResult();
        sm.toFinisher().run();
        return null;
    }

    /** Discards control-plane output when its query terminates unsuccessfully. */
    public void discardControlResult(String queryId) {
        if (resultService != null) resultService.discardUnopened(queryId);
    }

    @SuppressWarnings("unchecked")
    private void registerAll(StatementExecutionFactory<?> factory) {
        for (var type : factory.supportedTypes()) {
            registry.put((Class<? extends Statement>) type, factory);
        }
    }
}
