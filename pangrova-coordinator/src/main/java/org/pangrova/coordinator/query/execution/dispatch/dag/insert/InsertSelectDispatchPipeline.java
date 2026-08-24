package org.pangrova.coordinator.query.execution.dispatch.dag.insert;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.planner.PlannedQuery;

import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Handles INSERT INTO ... SELECT by compiling the SELECT into a Substrait plan
 * with a WriteOperator and dispatching to native workers. Workers write directly
 * to the target connector via NativeWriteExecutor — the coordinator receives only
 * a completion signal (row count, commit handle) and never touches data rows.
 */
public final class InsertSelectDispatchPipeline {

    private static final Logger LOG = Logger.getLogger(InsertSelectDispatchPipeline.class.getName());

    private InsertSelectDispatchPipeline() {}

    /**
     * Executes INSERT SELECT: compile SELECT to plan with WriteOperator,
     * dispatch to worker, wait for completion signal.
     *
     * <p>The worker writes SELECT output directly to the target connector's
     * page sink via NativeWriteExecutor. Data never transits through the coordinator.
     *
     * @param sm    query state machine for the INSERT SELECT statement
     * @param stmt  parsed INSERT SELECT AST node
     * @param deps  immutable dependency bundle (compiler, executor, dispatch, tracker, refresh)
     * @param config timeout and other pipeline configuration
     * @param parentSession coordinator session context
     * @return dispatch result with result endpoint, or null on fallback path
     */
    public static DispatchResult execute(
            QueryStateMachine sm,
            InsertSelectStatement stmt,
            InsertSelectContext deps,
            InsertSelectConfig config,
            CoordinatorSessionContext parentSession) {
        String subQueryId = "isq-" + java.util.UUID.randomUUID();
        try {
            registerWriteContextIfConfigured(deps, stmt, subQueryId);
            // Compile the full INSERT SELECT statement (not just the SELECT part)
            // so that Calcite produces a LogicalTableModify wrapping the SELECT plan.
            // dispatchPlan.enrichWithDmlWriteContext detects TableModify and sets
            // dmlWriteContext, routing through awaitTaskCompletion for write commit
            // metadata collection via the unified DAG dispatch path.
            String fullSql = new org.pangrova.parser.AstPrinter().print(stmt);
            PlannedQuery plannedQuery = deps.compiler().compileToPlannedQuery(fullSql);
            dispatchAndAwait(plannedQuery, deps.dispatchFn(), deps.taskTracker(),
                    config, parentSession, subQueryId);
            // Schema refresh and cache invalidation are best-effort operations.
            // Failures must not cause the INSERT SELECT to report failure after
            // data has already been successfully written by the worker.
            try {
                deps.schemaRefresh().run();
            } catch (Exception refreshEx) {
                LOG.warning("event=insert_select_schema_refresh_failed subQueryId="
                        + subQueryId + " error=" + refreshEx.getMessage());
            }
            if (deps.cacheInvalidator() != null) {
                try {
                    String targetTable = String.join(".", stmt.tableName().parts()).toLowerCase();
                    deps.cacheInvalidator().accept(targetTable);
                } catch (Exception cacheEx) {
                    LOG.warning("event=insert_select_cache_invalidation_failed subQueryId="
                            + subQueryId + " error=" + cacheEx.getMessage());
                }
            }
            return finishInsertSelect(sm, deps.resultService(), parentSession.queryId());
        } catch (Exception e) {
            cleanupSubQuery(deps.taskTracker(), subQueryId);
            throw e;
        }
    }

    private static DispatchResult finishInsertSelect(
            QueryStateMachine sm, CoordinatorResultService resultService, String queryId) {
        if (resultService != null) {
            return DispatchResult.fromLocal(
                    resultService.dispatchLocalFromTypedRows(queryId, List.of(), List.of(), sm.toFinisher()));
        }
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        return null;
    }

    /**
     * Cancels sub-query tasks and clears tracking state on failure.
     */
    static void cleanupSubQuery(CoordinatorTaskTracker taskTracker, String subQueryId) {
        try {
            Set<String> taskIds = taskTracker.getTaskIds(subQueryId);
            for (String taskId : taskIds) {
                taskTracker.clearTaskTracking(taskId);
            }
            LOG.warning("event=insert_select_sub_query_cleanup subQueryId=" + subQueryId
                    + " cancelledTasks=" + taskIds.size());
        } catch (Exception cleanupEx) {
            LOG.warning("event=insert_select_cleanup_failed subQueryId=" + subQueryId
                    + " error=" + cleanupEx.getMessage());
        }
    }

    /**
     * Dispatches the compiled plan to a worker and waits for completion.
     * The worker writes results directly to the target connector — the coordinator
     * only receives a completion signal.
     */
    private static void dispatchAndAwait(
            PlannedQuery plannedQuery,
            DispatchFunction dispatchFn,
            CoordinatorTaskTracker taskTracker,
            InsertSelectConfig config,
            CoordinatorSessionContext parentSession,
            String subQueryId) {
        CoordinatorSessionContext subSession = parentSession.withQueryId(subQueryId);
        QueryStateMachine subSm = new QueryStateMachine();
        subSm.tryTransition(QueryStateMachine.State.PLANNING);
        DispatchResult result = dispatchFn.dispatch(subSm, subSession, plannedQuery);
        if (result == null) {
            throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "INSERT SELECT sub-query dispatch failed");
        }
        // dispatchFn.dispatch routes through the unified DAG engine which
        // blocks until the worker task completes and returns results. By the time
        // dispatch returns, the native write task has finished and commit metadata
        // has been collected. No additional await is needed — the task is done.
    }

    /** Functional interface for dispatching a compiled query plan to workers. */
    @FunctionalInterface
    public interface DispatchFunction {
        DispatchResult dispatch(QueryStateMachine sm, CoordinatorSessionContext session, PlannedQuery plan);
    }

    /**
     * Registers write context with the commit tracker before dispatch so that
     * {@code TaskStatusReceiver} can collect commit metadata fragments from workers
     * and perform an atomic catalog commit via {@code finishDistributedWrite()}.
     *
     * <p>No-op when {@code writeCommitTracker} or {@code catalogRegistry} is null
     * (e.g., in test configurations without native write commit support).</p>
     */
    private static void registerWriteContextIfConfigured(
            InsertSelectContext deps,
            InsertSelectStatement stmt,
            String subQueryId) {
        var tracker = deps.writeCommitTracker();
        var registry = deps.catalogRegistry();
        if (tracker == null || registry == null) {
            return;
        }
        List<String> parts = stmt.tableName().parts();
        org.pangrova.spi.connector.metadata.table.QualifiedTableName ref =
                org.pangrova.spi.connector.metadata.table.QualifiedTableName.resolveOrNull(parts);
        if (ref == null) {
            LOG.warning("event=insert_select_write_context_skip reason=unresolvable_table parts=" + parts);
            return;
        }
        var outputHandle = beginWriteOperation(registry, ref, stmt.overwrite());
        var session = org.pangrova.coordinator.query.statement.TableResolver.buildSession(ref.catalog());
        tracker.registerWriteContext(subQueryId, ref.catalog(), outputHandle, session);
        LOG.info(() -> String.format(
                "event=insert_select_write_context_registered subQueryId=%s table=%s overwrite=%s",
                subQueryId, ref, stmt.overwrite()));
    }

    private static org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle beginWriteOperation(
            org.pangrova.metadata.catalog.CatalogRegistry registry,
            org.pangrova.spi.connector.metadata.table.QualifiedTableName ref,
            boolean overwrite) {
        org.pangrova.spi.connector.ConnectorSession session =
                org.pangrova.coordinator.query.statement.TableResolver.buildSession(ref.catalog());
        org.pangrova.metadata.catalog.CatalogHandle catalogHandle = registry.getCatalog(ref.catalog());
        org.pangrova.spi.connector.metadata.ConnectorMetadata metadata = catalogHandle.getMetadata(session);
        org.pangrova.spi.connector.metadata.table.ConnectorTableHandle tableHandle =
                org.pangrova.coordinator.query.statement.TableResolver.requireTableHandle(metadata, session, ref);
        return overwrite
                ? metadata.beginOverwrite(session, tableHandle)
                : metadata.beginInsert(session, tableHandle);
    }
}
