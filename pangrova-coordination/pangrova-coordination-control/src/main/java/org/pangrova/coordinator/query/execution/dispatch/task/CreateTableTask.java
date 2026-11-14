package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes CREATE TABLE by delegating to {@link DdlExecutor#executeCreateTable}.
 */
public final class CreateTableTask implements MetadataTask<CreateTableStatement> {

    private final DdlExecutor executor;

    public CreateTableTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCreateTable(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.DDL);
    }
}
