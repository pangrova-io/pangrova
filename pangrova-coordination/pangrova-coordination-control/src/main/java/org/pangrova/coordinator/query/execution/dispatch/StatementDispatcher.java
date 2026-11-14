package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.dispatch.StatementCompletionEvent;
import org.pangrova.query.dispatch.StatementCompletionListener;

import org.pangrova.execution.task.DispatchResult;

import org.pangrova.query.api.enums.QueryState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.api.ast.statement.DescribeCatalogStatement;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;

/**
 * Pure dispatcher for DDL/DML/utility statements.
 * Delegates execution to registered factories and fires completion events.
 * Contains minimal routing logic; statement rejection for unsupported types
 * ({@code rejectUnsupported}) is the only business decision in this class.
 */
public final class StatementDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(StatementDispatcher.class);

    private final Map<Class<? extends Statement>, StatementExecutionFactory<?>> registry;
    private final List<StatementCompletionListener> listeners;
    private volatile SqlCompiler compiler;
    private final CoordinatorResultService resultService;
    private final AccessControl accessControl;
    private final DdlLeadershipGuard ddlGuard;

    /** Creates a statement dispatcher from its execution dependencies. */
    public StatementDispatcher(StatementDispatcherContext deps) {
        this.compiler = deps.compiler();
        this.resultService = deps.resultService();
        this.accessControl = deps.accessControl();
        this.ddlGuard = deps.ddlGuard();
        this.registry = new ConcurrentHashMap<>();
        this.listeners = new CopyOnWriteArrayList<>(deps.listeners());
        deps.factories().forEach(this::registerAll);
    }

    /**
     * Attempts to dispatch a DDL/DML statement.
     * Returns null if the SQL is not a routable statement.
     */
    public DispatchResult tryDispatch(
            QueryStateMachine sm, String sql,
            String queryId, QuerySession session) {
        // Snapshot the volatile compiler reference once per dispatch to ensure
        // a consistent compiler version throughout this request's lifecycle.
        SqlCompiler currentCompiler = this.compiler;
        Statement ast = parseAstOrNull(sql);
        if (ast == null) {
            return null;
        }

        if (!isRoutable(ast)) {
            return null;
        }
        // DDL leader routing guard: when enabled, rejects DDL on non-leader nodes
        // with HTTP 307 redirect. When disabled (default), DDL runs on any coordinator.
        if (ddlGuard != null) {
            ddlGuard.requireLeaderForDdl(ast);
        }
        rejectUnsupported(ast);
        StatementExecutionFactory<?> factory = registry.get(ast.getClass());
        if (factory == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No factory registered for: " + ast.getClass().getSimpleName());
        }
        checkPermission(ast, session);
        var context = new StatementExecutionContext(sm, queryId, session);
        StatementResult result = executeUnchecked(factory, ast, context);
        fireCompletionEvent(queryId, ast, result, session);
        return toDispatchResult(result, queryId, sm);
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
        rejectUnsupported(ast);
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

    /** Updates the compiler reference after schema changes. */
    public void updateCompiler(SqlCompiler newCompiler) {
        this.compiler = newCompiler;
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

    /**
     * Rejects unsupported statement types with a descriptive error message
     * containing the SQL syntax name. Called before factory lookup so that
     * unsupported statements fail fast with a clear message.
     *
     * <p>Data-driven: unsupported types are declared in a static map rather
     * than an if-else chain, making it easy to add new unsupported types.</p>
     */
    private static final Map<Class<? extends Statement>, String> UNSUPPORTED_STATEMENTS = Map.of(
            DescribeCatalogStatement.class, "DESCRIBE CATALOG",
            AlterCatalogStatement.class, "ALTER CATALOG"
    );

    private static void rejectUnsupported(Statement stmt) {
        String sqlName = UNSUPPORTED_STATEMENTS.get(stmt.getClass());
        if (sqlName != null) {
            throw new UnsupportedOperationException(sqlName + " is not supported");
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Statement> StatementResult executeUnchecked(
            StatementExecutionFactory<T> factory, Statement stmt,
            StatementExecutionContext context) {
        return factory.execute((T) stmt, context);
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
        if (resultService != null) {
            var control = resultService.dispatchControlPlaneRows(
                    queryId, result.rows(), result.columnNames(), () -> { });
            // The response bytes and endpoint must be visible before FINISHED:
            // Native CTAS may already have completed, while an outer DDL still
            // needs to publish its metadata head and acknowledge the statement.
            sm.setResultEndpoint(control.resultEndpoint());
            sm.toFinisher().run();
            return control;
        }
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.tryTransition(QueryState.FINISHING);
        sm.tryTransition(QueryState.FINISHED);
        return null;
    }

    private Statement parseAstOrNull(String sql) {
        try {
            return compiler.parseToAst(sql);
        } catch (Exception e) {
            LOG.debug("event=parser_fallback sql_length={} error={}", sql.length(), e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private void registerAll(StatementExecutionFactory<?> factory) {
        for (var type : factory.supportedTypes()) {
            registry.put((Class<? extends Statement>) type, factory);
        }
    }
}
