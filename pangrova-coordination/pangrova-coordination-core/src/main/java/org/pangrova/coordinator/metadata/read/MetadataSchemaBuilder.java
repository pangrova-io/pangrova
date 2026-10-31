package org.pangrova.coordinator.metadata.read;

import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.connector.spi.ConnectorSession;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges MetadataService and CatalogRegistry to Calcite's schema mechanism.
 * Creates a CalciteSchema tree from registered catalogs so Calcite validator
 * can resolve table references during SQL compilation.
 */
public final class MetadataSchemaBuilder {

    private final MetadataService metadataService;
    private final CatalogRegistry catalogRegistry;

    public MetadataSchemaBuilder(MetadataService metadataService) {
        this(metadataService, null);
    }

    public MetadataSchemaBuilder(MetadataService metadataService, CatalogRegistry catalogRegistry) {
        this.metadataService = metadataService;
        this.catalogRegistry = catalogRegistry;
    }

    /**
     * Builds a CalciteSchema root containing all registered catalogs as sub-schemas.
     * Each catalog exposes its schemas as Calcite sub-schemas for table resolution.
     *
     * <p>Uses {@code cache=false} to create a non-caching schema instead of the
     * default caching variant. The caching variant snapshots table names on first
     * access and never re-queries the underlying Schema, which prevents newly created
     * tables from being visible to subsequent queries. With the non-caching variant,
     * named lookups use the connector's direct table API; explicit table enumeration
     * uses {@link LazyMetadataSchema#getTableMap()} and honors DDL invalidation.</p>
     */
    public CalciteSchema buildRootSchema(ConnectorSession authenticatedSession) {
        return buildRootSchema(authenticatedSession, catalogRegistry == null ? null
                : new PlanningReadBindings(catalogRegistry, authenticatedSession));
    }

    /** Builds schema and cost metadata with the same query-local selected read owner. */
    public CalciteSchema buildRootSchema(ConnectorSession authenticatedSession, PlanningReadBindings readBindings) {
        ConnectorSession session = PlanningConnectorSession.bind(authenticatedSession,
                authenticatedSession.getCatalog().orElse(null),
                authenticatedSession.getSchema().orElse(null));
        MetadataSessionContext metadataSession = new MetadataSessionContext(
                session.getUser(), session.getCatalog().orElse(null), session.getSchema().orElse(null),
                null, null, session.getProperties(), session.getProperties().get("tenant_id"), null,
                session.getQueryAuthority().map(org.pangrova.connector.spi.ConnectorQueryAuthority::queryId)
                        .orElseGet(() -> session.getProperties().get("queryId")), session.getQueryAuthority());
        Map<String, org.apache.calcite.schema.Schema> catalogSchemas = new ConcurrentHashMap<>();
        AbstractSchema root = new AbstractSchema() {
            @Override
            protected Map<String, org.apache.calcite.schema.Schema> getSubSchemaMap() {
                Map<String, org.apache.calcite.schema.Schema> visible = new LinkedHashMap<>();
                for (String catalog : metadataService.listCatalogs(metadataSession)) {
                    visible.put(catalog, catalogSchemas.computeIfAbsent(catalog,
                            name -> catalogSchema(session, metadataSession, name, readBindings)));
                }
                return visible;
            }
        };
        return CalciteSchema.createRootSchema(false, false, "", root);
    }

    private AbstractSchema catalogSchema(ConnectorSession session, MetadataSessionContext metadataSession,
                                 String catalog, PlanningReadBindings readBindings) {
        Map<String, org.apache.calcite.schema.Schema> schemaInstances = new ConcurrentHashMap<>();
        return new AbstractSchema() {
            @Override
            protected Map<String, org.apache.calcite.schema.Schema> getSubSchemaMap() {
                ConnectorSession catalogSession = PlanningConnectorSession.bind(session, catalog, null);
                List<String> names = catalogRegistry == null
                        ? metadataService.listSchemas(metadataSession, catalog)
                        : catalogRegistry.getCatalog(catalog).getMetadata(catalogSession)
                                .listSchemaNames(catalogSession);
                Map<String, org.apache.calcite.schema.Schema> visible = new LinkedHashMap<>();
                for (String name : names) {
                    visible.put(name, schemaInstances.computeIfAbsent(name,
                            schema -> new LazyMetadataSchema(catalogRegistry, catalog, schema, session, readBindings)));
                }
                return visible;
            }
        };
    }

    /**
     * Bumps the global DDL generation counter, causing all LazyMetadataSchema
     * instances to refresh their table cache on next access.
     * Call after CREATE/DROP/ALTER TABLE to ensure Calcite sees the change.
     */
    public static void invalidateTableCaches() {
        LazyMetadataSchema.bumpDdlGeneration();
    }
}
