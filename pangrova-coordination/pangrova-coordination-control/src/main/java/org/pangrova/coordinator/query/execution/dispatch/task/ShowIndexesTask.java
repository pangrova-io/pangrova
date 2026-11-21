package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
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
        return StatementResult.fromRows(rows, null, StatementCategory.INDEX);
    }
}
