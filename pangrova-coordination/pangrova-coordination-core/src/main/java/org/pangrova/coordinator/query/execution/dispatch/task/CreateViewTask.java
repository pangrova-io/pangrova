package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.parser.ast.statement.CreateViewStatement;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes CREATE VIEW by resolving the catalog and delegating to
 * {@link ConnectorMetadata#createView}.
 */
public final class CreateViewTask implements MetadataTask<CreateViewStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateViewTask.class);

    private final CatalogRegistry catalogRegistry;

    public CreateViewTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    @Override
    public StatementResult execute(CreateViewStatement statement, StatementExecutionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(statement.viewName().parts(), context.session());
        ConnectorSession session = TableResolver.buildSession(context.session(), ref.catalog(), ref.schema());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(ref.catalog()).getMetadata(session);
        SchemaTableName viewName = new SchemaTableName(ref.schema(), ref.table());

        if (statement.ifNotExists()) {
            var existing = metadata.getViewDefinition(session, viewName);
            if (existing.isPresent()) {
                LOG.info("event=view_exists_skip view={}", ref);
                return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
            }
        }

        metadata.createView(session, viewName, statement.viewSql());
        LOG.info("event=view_created view={}", ref);
        return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
    }
}
