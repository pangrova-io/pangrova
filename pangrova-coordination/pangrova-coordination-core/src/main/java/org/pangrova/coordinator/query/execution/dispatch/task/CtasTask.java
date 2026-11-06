package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.core.format.AstPrinter;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.planner.core.analysis.PlannedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes CREATE TABLE AS SELECT (CTAS) by:
 * <ol>
 *   <li>Compiling the inner SELECT to derive output schema (column names + types)</li>
 *   <li>Creating the target table via DDL executor</li>
 *   <li>Dispatching an INSERT SELECT through the native DML pipeline (workers write directly)</li>
 * </ol>
 *
 * <p>The coordinator never touches data rows — schema derivation uses only plan metadata,
 * and data insertion is performed entirely by native workers via Substrait plan execution.</p>
 */
public final class CtasTask implements MetadataTask<CreateTableAsSelectStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(CtasTask.class);

    private final DdlExecutor ddlExecutor;
    private final NativeDmlCompileFunction dmlDispatch;

    public CtasTask(DdlExecutor ddlExecutor, NativeDmlCompileFunction dmlDispatch) {
        this.ddlExecutor = ddlExecutor;
        this.dmlDispatch = dmlDispatch;
    }

    @Override
    public StatementResult execute(CreateTableAsSelectStatement statement, StatementExecutionContext context) {
        // 1. Compile the inner SELECT to derive output schema
        String innerSql = new AstPrinter().print(statement.query());
        PlannedQuery plannedQuery = context.compiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(context.session()))
                .compileToPlannedQuery(innerSql);

        // 2. Build column definitions from derived schema
        List<CreateTableStatement.ColumnDefinition> columns = deriveColumnDefinitions(plannedQuery);

        // 3. Create the target table
        CreateTableStatement createStmt = new CreateTableStatement(
                statement.tableName(), columns, statement.ifNotExists(),
                List.of(), List.of(), Optional.empty(), Map.of());
        ddlExecutor.executeCreateTable(createStmt, context.session());
        LOG.info("event=ctas_table_created table={}", String.join(".", statement.tableName().parts()));

        // 4. Route data insertion through native DML pipeline (workers write directly)
        InsertSelectStatement insertStmt = buildInsertSelect(statement);
        DispatchResult result = dmlDispatch.apply(
                context.stateMachine(), context.session(), insertStmt);
        if (result == null) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "CTAS data insertion dispatch failed for table: "
                            + String.join(".", statement.tableName().parts()));
        }

        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        LOG.info("event=ctas_complete table={}", mutatedTable);
        return new StatementResult(List.of(), List.of(), mutatedTable, StatementCategory.DDL);
    }

    /** Derives the actual CTAS declaration from the planned output, preserving parameterized storage types. */
    public static List<CreateTableStatement.ColumnDefinition> deriveColumnDefinitions(PlannedQuery plan) {
        List<String> names = plan.queryOutputColumns();
        List<String> types = plan.queryOutputColumnTypeNames();
        if (names.size() != types.size()) {
            throw new IllegalStateException(
                    "Column name/type count mismatch: " + names.size() + " names vs " + types.size() + " types");
        }
        return java.util.stream.IntStream.range(0, names.size())
                .mapToObj(i -> new CreateTableStatement.ColumnDefinition(
                        names.get(i), org.pangrova.parser.core.build.type.RawSqlTypeParser.parse(types.get(i))))
                .toList();
    }

    private static InsertSelectStatement buildInsertSelect(CreateTableAsSelectStatement ctas) {
        Statement query = ctas.query();
        SetQuery setQuery;
        if (query instanceof SetQuery sq) {
            setQuery = sq;
        } else if (query instanceof Query q) {
            // Simple SELECT without set operations — wrap in a single-query SetQuery
            setQuery = new SetQuery(List.of(q), List.of());
        } else {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "CTAS inner query must be a SELECT statement, got: "
                            + query.getClass().getSimpleName());
        }
        return new InsertSelectStatement(
                ctas.tableName(), Optional.empty(), false, setQuery);
    }
}
