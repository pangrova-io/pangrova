package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.query.api.statement.StatementCategory;

import org.pangrova.query.core.statement.StatementTableTargets;

import org.pangrova.metadata.core.resolution.ConnectorTableLookup;
import org.pangrova.query.core.task.DispatchResult;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.core.format.AstPrinter;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.statement.ExplainStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.types.value.TypedRow;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Executes statements that require compilation to a Substrait plan and
 * dispatch to native workers: INSERT, DELETE, UPDATE, MERGE, and EXPLAIN.
 *
 * <p>All relational DML, including VALUES relations, is routed through the native DML
 * dispatch pipeline via {@link NativeDmlCompileFunction}. EXPLAIN compiles
 * the inner statement and returns the plan text.</p>
 */
public final class PlanExecutionFactory implements StatementExecutionFactory<Statement> {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(PlanExecutionFactory.class);

    private final NativeDmlCompileFunction dmlDispatch;
    private final Consumer<String> writeCompleteCallback;
    private final BiFunction<String, QuerySession, String> explainCompiler;
    private final org.pangrova.metadata.catalog.registry.CatalogRegistry catalogRegistry;
    private final org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
            generatedValueMaterialization;

    /**
     * @param ctx bundled dependencies for plan execution
     */
    public PlanExecutionFactory(PlanExecutionFactoryContext ctx) {
        this.dmlDispatch = checkNotNull(ctx.dmlDispatch(), "dmlDispatch");
        this.writeCompleteCallback = ctx.writeCompleteCallback();
        this.explainCompiler = checkNotNull(ctx.explainCompiler(), "explainCompiler");
        this.catalogRegistry = ctx.catalogRegistry();
        this.generatedValueMaterialization = ctx.generatedValueMaterialization();
    }

    @Override
    public Set<Class<? extends Statement>> supportedTypes() {
        return Set.of(DeleteStatement.class, UpdateStatement.class,
                MergeStatement.class, InsertStatement.class,
                InsertOverwriteStatement.class, InsertSelectStatement.class,
                ExplainStatement.class);
    }

    @Override
    public StatementResult execute(Statement statement, StatementExecutionContext ctx) {
        if (statement instanceof ExplainStatement s) {
            return executeExplain(s, ctx);
        }
        if (statement instanceof InsertStatement
                || statement instanceof InsertOverwriteStatement
                || statement instanceof InsertSelectStatement) {
            return executeDml(statement, ctx);
        }
        if (statement instanceof DeleteStatement delStmt) {
            return executeDml(delStmt, ctx);
        }
        if (statement instanceof UpdateStatement updStmt) {
            return executeDml(updStmt, ctx);
        }
        if (statement instanceof MergeStatement mergeStmt) {
            return executeDml(mergeStmt, ctx);
        }
        throw new UnsupportedOperationException(
                "Unsupported plan statement: " + statement.getClass().getSimpleName());
    }

    private StatementResult executeDml(Statement statement, StatementExecutionContext ctx) {
        validateInsertValuesGovernance(statement, ctx.session());
        requireGeneratedColumnPlanIfNeeded(statement, ctx.session());
        // The Native write can finish before the dispatcher publishes its bounded
        // control-plane acknowledgement. Keep FINISHED hidden until that endpoint exists.
        checkNotNull(ctx.stateMachine(), "DML query state")
                .deferTerminalUntilControlPlaneResult();
        DispatchResult result = dmlDispatch.apply(ctx.stateMachine(), ctx.session(),
                statement);
        if (result == null) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "Native DML dispatch returned null for "
                            + statement.getClass().getSimpleName());
        }
        var mutatedRef = StatementTableTargets.resolve(statement, ctx.session());
        String mutatedTable = mutatedRef == null ? null : mutatedRef.toString();
        if (writeCompleteCallback != null && mutatedTable != null) {
            try {
                writeCompleteCallback.accept(mutatedTable);
            } catch (RuntimeException callbackFailure) {
                // The fenced catalog commit has already completed. A best-effort
                // maintenance notification must not turn that durable success into
                // a client-visible write failure and trigger an unsafe retry.
                LOG.warn("event=write_complete_callback_failed table={} queryId={} error={}",
                        mutatedTable, ctx.session().queryId(), callbackFailure.getMessage());
            }
        }
        return new StatementResult(List.of(), List.of(),
                mutatedTable, StatementCategory.DML);
    }

    private void validateInsertValuesGovernance(
            Statement statement, QuerySession coordinatorSession) {
        final java.util.Optional<java.util.List<String>> requestedColumns;
        final java.util.List<java.util.List<org.pangrova.parser.api.ast.Expression>> rows;
        if (statement instanceof InsertStatement insert) {
            requestedColumns = insert.columns();
            rows = insert.values();
        } else if (statement instanceof InsertOverwriteStatement overwrite) {
            requestedColumns = overwrite.columns();
            rows = overwrite.values();
        } else {
            return;
        }
        if (rows.isEmpty()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "INSERT VALUES requires at least one row");
        }
        org.pangrova.connector.api.metadata.table.QualifiedTableName ref =
                StatementTableTargets.resolve(statement, coordinatorSession);
        org.pangrova.connector.api.session.ConnectorSession session =
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                        coordinatorSession, ref.catalog(), ref.schema());
        org.pangrova.connector.spi.metadata.ConnectorMetadata metadata =
                catalogRegistry.getCatalog(ref.catalog()).getMetadata(session);
        org.pangrova.connector.api.metadata.table.ConnectorTableHandle table =
                ConnectorTableLookup.requireTableHandle(
                        metadata, session, ref);
        java.util.List<String> columns =
                ConnectorTableLookup.resolveInsertColumns(
                        requestedColumns, metadata, session, table);
        org.pangrova.coordinator.query.statement.dml.FileColumnMimeValidator.validate(
                metadata.getTableMetadata(session, table), columns, rows);
        org.pangrova.coordinator.query.statement.dml.ManagedFileWriteGuard.requireSupported(
                metadata.getTableMetadata(session, table), columns, rows);
    }

    private void requireGeneratedColumnPlanIfNeeded(
            Statement statement, QuerySession session) {
        if (statement instanceof DeleteStatement || catalogRegistry == null) {
            return;
        }
        org.pangrova.connector.api.metadata.table.QualifiedTableName ref =
                StatementTableTargets.resolve(statement, session);
        if (ref == null) {
            return;
        }
        org.pangrova.connector.api.session.ConnectorSession connectorSession =
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                        session, ref.catalog(), ref.schema());
        org.pangrova.connector.spi.metadata.ConnectorMetadata metadata =
                catalogRegistry.getCatalog(ref.catalog()).getMetadata(connectorSession);
        org.pangrova.connector.api.metadata.table.ConnectorTableHandle tableHandle =
                ConnectorTableLookup.requireTableHandle(
                        metadata, connectorSession, ref);
        org.pangrova.coordinator.query.statement.dml.GeneratedColumnWriteGuard
                .requireNativeMaterializationPlan(
                        metadata, connectorSession, tableHandle, ref,
                        statement.getClass().getSimpleName(),
                        generatedValueMaterialization,
                        session.userIdentity());
    }

    private StatementResult executeExplain(ExplainStatement stmt,
                                            StatementExecutionContext ctx) {
        String innerSql = new AstPrinter().print(stmt.innerStatement());
        String planText = explainCompiler.apply(innerSql, ctx.session());
        List<TypedRow> rows = List.of(
                TypedRow.builder().putString("plan", planText).build());
        return new StatementResult(rows, List.of("plan"),
                null, StatementCategory.EXPLAIN);
    }
}
