package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.statement.DropViewStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes DROP VIEW by resolving the catalog and delegating to
 * {@link ConnectorMetadata#dropView}.
 */
public final class DropViewTask implements MetadataTask<DropViewStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(DropViewTask.class);

    private final CatalogRegistry catalogRegistry;

    public DropViewTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    @Override
    public StatementResult execute(DropViewStatement statement, StatementExecutionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(statement.viewName().parts(), context.session());
        ConnectorSession session = TableResolver.buildSession(context.session(), ref.catalog(), ref.schema());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(ref.catalog()).getMetadata(session);
        SchemaTableName viewName = new SchemaTableName(ref.schema(), ref.table());

        if (statement.ifExists()) {
            var existing = metadata.getViewDefinition(session, viewName);
            if (existing.isEmpty()) {
                LOG.info("event=view_not_found_skip view={}", ref);
                return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
            }
        }

        metadata.dropView(session, viewName);
        LOG.info("event=view_dropped view={}", ref);
        return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
    }
}
