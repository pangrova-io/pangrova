package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.connector.spi.data.value.TypedRow;

/**
 * Executes SHOW TABLES by delegating to {@link ShowDescribeExecutor#executeShowTables}.
 */
public final class ShowTablesTask implements MetadataTask<ShowTablesStatement> {

    private final ShowDescribeExecutor executor;

    public ShowTablesTask(ShowDescribeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowTablesStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowTables(statement, context.session());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.SHOW);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
