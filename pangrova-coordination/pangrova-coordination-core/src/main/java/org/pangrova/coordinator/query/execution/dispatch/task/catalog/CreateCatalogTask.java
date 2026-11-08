package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.api.ast.CreateCatalogStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes CREATE CATALOG by delegating to {@link DdlExecutor#executeCreateCatalog}.
 */
public final class CreateCatalogTask implements MetadataTask<CreateCatalogStatement> {

    private final DdlExecutor executor;

    public CreateCatalogTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateCatalogStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCreateCatalog(statement);
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.DDL);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
