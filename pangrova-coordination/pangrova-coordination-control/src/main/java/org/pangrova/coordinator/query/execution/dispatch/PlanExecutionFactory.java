package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.dispatch.StatementResult;

import org.pangrova.query.api.enums.StatementCategory;

import org.pangrova.query.statement.StatementTableTargets;

import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.execution.task.DispatchResult;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.coordinator.query.statement.dml.FileColumnMimeValidator;
import org.pangrova.coordinator.query.statement.dml.GeneratedColumnWriteGuard;
import org.pangrova.coordinator.query.statement.dml.ManagedFileWriteGuard;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.statement.ExplainStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.types.value.TypedRow;

import static org.pangrova.common.validation.Preconditions.checkNotNull;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;

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
    public boolean usesCoordinatorStatementAuthority(Statement statement) {
        // EXPLAIN produces a local Flight result. Native DML is fenced by task issuance and the write journal.
        return statement instanceof ExplainStatement;
    }

    @Override
    public StatementResult execute(Statement statement, StatementExecutionContext ctx) {
        return DAGCompletionEvaluator.awaitResult(start(statement, ctx), ctx.queryId(), () -> { });
    }

    @Override
    public CompletionStage<StatementResult> start(Statement statement, StatementExecutionContext ctx) {
        if (statement instanceof ExplainStatement s) {
            return CompletableFuture.completedFuture(executeExplain(s, ctx));
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

    private CompletionStage<StatementResult> executeDml(Statement statement, StatementExecutionContext ctx) {
        validateDmlAdmission(statement, ctx.session());
        // The Native write can finish before the dispatcher publishes its bounded
        // control-plane acknowledgement. Keep FINISHED hidden until that endpoint exists.
        checkNotNull(ctx.stateMachine(), "DML query state")
                .deferTerminalUntilControlPlaneResult();
        return dmlDispatch.apply(ctx.stateMachine(), ctx.session(), statement)
                .thenApply(result -> completeDml(statement, ctx, result));
    }

    private StatementResult completeDml(Statement statement, StatementExecutionContext ctx, DispatchResult result) {
        if (result == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
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

    private void validateDmlAdmission(Statement statement, QuerySession querySession) {
        if (statement instanceof DeleteStatement) return;
        List<List<Expression>> rows = null;
        Optional<List<String>> explicitColumns = Optional.empty();
        if (statement instanceof InsertStatement insert) {
            rows = insert.values();
            explicitColumns = insert.columns();
        } else if (statement instanceof InsertOverwriteStatement overwrite) {
            rows = overwrite.values();
            explicitColumns = overwrite.columns();
        }
        if (rows != null) {
            if (rows.isEmpty()) {
                throw new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "INSERT VALUES requires at least one row");
            }
            checkNotNull(catalogRegistry, "INSERT VALUES requires catalog metadata");
        }
        if (catalogRegistry == null) return;
        var ref = StatementTableTargets.resolve(statement, querySession);
        if (ref == null) return;
        var connectorSession = SessionContextConverter.toConnectorSession(querySession, ref.catalog(), ref.schema());
        var metadata = catalogRegistry.getCatalog(ref.catalog()).getMetadata(connectorSession);
        // The write allocation independently revalidates its authority before opening the output handle.
        var table = metadata.describeNamedTable(connectorSession, new SchemaTableName(ref.schema(), ref.table()))
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_TABLE_NOT_FOUND, "Table not found: " + ref));
        if (rows != null) {
            List<String> columns = explicitColumns.orElse(table.columns());
            FileColumnMimeValidator.validate(table, columns, rows);
            ManagedFileWriteGuard.requireSupported(table, columns, rows);
        }
        GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                table, connectorSession, ref, statement.getClass().getSimpleName(),
                generatedValueMaterialization, querySession.userIdentity());
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
