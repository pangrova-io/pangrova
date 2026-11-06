package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.api.ast.statement.ShowCatalogsStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW CATALOGS by delegating to {@link ShowDescribeExecutor#executeShowCatalogs}.
 */
public final class ShowCatalogsTask implements MetadataTask<ShowCatalogsStatement> {

    private final ShowDescribeExecutor executor;

    public ShowCatalogsTask(ShowDescribeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowCatalogsStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowCatalogs();
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.SHOW);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
