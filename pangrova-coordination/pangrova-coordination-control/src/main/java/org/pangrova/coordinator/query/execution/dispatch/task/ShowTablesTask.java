package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.api.ast.statement.ShowTablesStatement;
import org.pangrova.types.value.TypedRow;

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
        return StatementResult.fromRows(rows, null, StatementCategory.SHOW);
    }
}
