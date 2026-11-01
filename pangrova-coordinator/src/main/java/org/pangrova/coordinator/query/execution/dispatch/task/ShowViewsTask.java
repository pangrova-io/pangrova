package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.parser.ast.statement.ShowViewsStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
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
        String catalog = parts.size() >= 2 ? parts.get(0) : TableResolver.defaultCatalog(context.session());
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = TableResolver.buildSession(context.session(), catalog, schema);
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
