package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.api.ast.statement.CreateSchemaStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes CREATE SCHEMA by delegating to {@link DdlExecutor#executeCreateSchema}.
 */
public final class CreateSchemaTask implements MetadataTask<CreateSchemaStatement> {

    private final DdlExecutor executor;

    public CreateSchemaTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateSchemaStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCreateSchema(statement, context.session());
        return StatementResult.fromRows(rows, null, StatementCategory.DDL);
    }
}
