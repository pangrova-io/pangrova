package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.connector.spi.data.value.TypedRow;

/**
 * Executes DESCRIBE TABLE by delegating to {@link ShowDescribeExecutor#executeDescribeTable}.
 */
public final class DescribeTableTask implements MetadataTask<DescribeTableStatement> {

    private final ShowDescribeExecutor executor;

    public DescribeTableTask(ShowDescribeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DescribeTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeDescribeTable(statement, context.session());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.SHOW);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
