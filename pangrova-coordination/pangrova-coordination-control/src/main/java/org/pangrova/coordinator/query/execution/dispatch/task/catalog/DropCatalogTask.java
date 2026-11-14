package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.api.ast.statement.DropCatalogStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DROP CATALOG by delegating to {@link DdlExecutor#executeDropCatalog}.
 */
public final class DropCatalogTask implements MetadataTask<DropCatalogStatement> {

    private final DdlExecutor executor;

    public DropCatalogTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropCatalogStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeDropCatalog(statement, context.session());
        return StatementResult.fromRows(rows, null, StatementCategory.DDL);
    }
}
