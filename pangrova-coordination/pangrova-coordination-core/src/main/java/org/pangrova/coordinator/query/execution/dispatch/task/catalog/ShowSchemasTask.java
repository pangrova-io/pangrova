package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.api.ast.ShowSchemasStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW SCHEMAS by delegating to {@link ShowDescribeExecutor#executeShowSchemas}.
 */
public final class ShowSchemasTask implements MetadataTask<ShowSchemasStatement> {

    private final ShowDescribeExecutor executor;

    public ShowSchemasTask(ShowDescribeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowSchemasStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowSchemas(statement, context.session());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.SHOW);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
