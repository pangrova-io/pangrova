package org.pangrova.coordinator.query.execution.dispatch.task.catalog;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.api.ast.CreateCatalogStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.api.ast.statement.DescribeCatalogStatement;
import org.pangrova.parser.api.ast.statement.DropCatalogStatement;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementResult;

import java.util.List;
import java.util.Map;

/** Registers the supported catalog DDL and inspection SQL handlers on the coordinator. */
public final class CatalogStatementTasks {
    private CatalogStatementTasks() {}

    /** Registers the metadata tasks that execute catalog DDL and DESCRIBE CATALOG statements. */
    public static void register(Map<Class<? extends Statement>, MetadataTask<?>> tasks,
                                DdlExecutor ddl, ShowDescribeExecutor inspection) {
        tasks.put(CreateCatalogStatement.class, new CreateCatalogTask(ddl));
        tasks.put(DropCatalogStatement.class, new DropCatalogTask(ddl));
        tasks.put(AlterCatalogStatement.class, (MetadataTask<AlterCatalogStatement>) (statement, context) -> {
            ddl.alterCatalog(statement, context.session());
            return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
        });
        tasks.put(DescribeCatalogStatement.class, (MetadataTask<DescribeCatalogStatement>) (statement, context) -> {
            var row = inspection.describeCatalog(statement.name(), context.session());
            return new StatementResult(List.of(row), List.copyOf(row.columnNames()), null, StatementCategory.SHOW);
        });
    }
}
