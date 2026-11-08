package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.parser.api.ast.statement.DropViewStatement;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
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
        QualifiedTableName ref = SessionTableNames.resolveTableRef(statement.viewName().parts(), context.session());
        ConnectorSession session = SessionContextConverter.toConnectorSession(context.session(), ref.catalog(),
                ref.schema());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(ref.catalog()).getMetadata(session);
        SchemaTableName viewName = new SchemaTableName(ref.schema(), ref.table());

        metadata.dropView(session, viewName, statement.ifExists());
        LOG.info("event=view_dropped view={}", ref);
        return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
    }
}
