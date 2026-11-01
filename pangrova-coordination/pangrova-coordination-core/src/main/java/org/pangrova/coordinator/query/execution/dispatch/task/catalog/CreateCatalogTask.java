package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

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
