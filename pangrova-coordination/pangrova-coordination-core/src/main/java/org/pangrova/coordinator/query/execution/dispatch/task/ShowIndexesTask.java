package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.ShowIndexesStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW INDEXES by delegating to {@link IndexDdlExecutor#executeShowIndexes}.
 */
public final class ShowIndexesTask implements MetadataTask<ShowIndexesStatement> {

    private final IndexDdlExecutor executor;

    public ShowIndexesTask(IndexDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowIndexesStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowIndexes(
                statement, context.session());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.INDEX);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
