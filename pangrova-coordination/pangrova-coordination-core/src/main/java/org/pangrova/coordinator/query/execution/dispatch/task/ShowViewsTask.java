package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.parser.api.ast.statement.ShowViewsStatement;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SHOW VIEWS by resolving the catalog and delegating to
 * {@link ConnectorMetadata#listViews}.
 */
public final class ShowViewsTask implements MetadataTask<ShowViewsStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(ShowViewsTask.class);

    private final CatalogRegistry catalogRegistry;

    public ShowViewsTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    @Override
    public StatementResult execute(ShowViewsStatement statement, StatementExecutionContext context) {
        List<String> parts = statement.qualifiedName();
        String catalog = parts.size() >= 2 ? parts.get(0) : SessionTableNames.defaultCatalog(context.session());
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = SessionContextConverter.toConnectorSession(context.session(), catalog, schema);
        ConnectorMetadata metadata = catalogRegistry.getCatalog(catalog).getMetadata(session);

        List<SchemaTableName> views = metadata.listViews(session, Optional.of(schema));
        List<TypedRow> rows = views.stream()
                .map(v -> TypedRow.builder().putString("view_name", v.table()).build())
                .toList();
        List<String> columnNames = List.of("view_name");
        LOG.info("event=show_views catalog={} schema={} count={}", catalog, schema, views.size());
        return new StatementResult(rows, columnNames, null, StatementCategory.SHOW);
    }
}
