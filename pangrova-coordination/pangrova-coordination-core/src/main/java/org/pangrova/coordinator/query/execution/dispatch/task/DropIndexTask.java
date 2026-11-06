package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.DropIndexStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DROP INDEX by delegating to {@link IndexDdlExecutor#executeDropIndex}.
 */
public final class DropIndexTask implements MetadataTask<DropIndexStatement> {

    private final IndexDdlExecutor executor;

    public DropIndexTask(IndexDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropIndexStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeDropIndex(
                statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.INDEX);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
