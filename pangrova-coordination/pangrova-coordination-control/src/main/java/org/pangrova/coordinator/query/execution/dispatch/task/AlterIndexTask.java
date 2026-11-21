package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.AlterIndexStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes ALTER INDEX by delegating to {@link IndexDdlExecutor#executeAlterIndex}.
 */
public final class AlterIndexTask implements MetadataTask<AlterIndexStatement> {

    private final IndexDdlExecutor executor;

    public AlterIndexTask(IndexDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AlterIndexStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeAlterIndex(
                statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.INDEX);
    }
}
