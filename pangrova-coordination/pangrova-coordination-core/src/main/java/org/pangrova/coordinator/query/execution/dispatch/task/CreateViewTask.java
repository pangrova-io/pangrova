package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.parser.api.ast.statement.CreateViewStatement;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
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
        QualifiedTableName ref = SessionTableNames.resolveTableRef(statement.viewName().parts(), context.session());
        ConnectorSession session = SessionContextConverter.toConnectorSession(context.session(), ref.catalog(),
                ref.schema());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(ref.catalog()).getMetadata(session);
        SchemaTableName viewName = new SchemaTableName(ref.schema(), ref.table());

        if (statement.ifNotExists() && statement.orReplace()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "CREATE VIEW cannot combine OR REPLACE with IF NOT EXISTS");
        }
        var mode = statement.orReplace()
                ? org.pangrova.connector.api.metadata.view.ViewCreateMode.CREATE_OR_REPLACE
                : statement.ifNotExists() ? org.pangrova.connector.api.metadata.view.ViewCreateMode.IF_NOT_EXISTS
                : org.pangrova.connector.api.metadata.view.ViewCreateMode.CREATE;
        metadata.createView(session, viewName, statement.viewSql(), mode);
        LOG.info("event=view_created view={}", ref);
        return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
    }
}
