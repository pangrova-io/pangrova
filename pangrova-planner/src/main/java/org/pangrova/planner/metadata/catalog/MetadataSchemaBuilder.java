package org.pangrova.planner.metadata.catalog;

import org.pangrova.metadata.api.catalog.CatalogReadAccess;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.connector.api.session.ConnectorSession;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.LazyReference;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Bridges MetadataService and CatalogReadAccess to Calcite's schema mechanism.
 * Creates a CalciteSchema tree from registered catalogs so Calcite validator
 * can resolve table references during SQL compilation.
 */
public final class MetadataSchemaBuilder {

    private final MetadataService metadataService;
    private final CatalogReadAccess catalogRegistry;

    public MetadataSchemaBuilder(MetadataService metadataService) {
        this(metadataService, null);
    }

    public MetadataSchemaBuilder(MetadataService metadataService, CatalogReadAccess catalogRegistry) {
        this.metadataService = metadataService;
        this.catalogRegistry = catalogRegistry;
    }

    /**
     * Builds a CalciteSchema root containing all registered catalogs as sub-schemas.
     * Each catalog exposes its schemas as Calcite sub-schemas for table resolution.
     *
     * <p>Uses {@code cache=false} to create a non-caching schema instead of the
     * default caching variant. Each admitted query lazily binds its catalog and
     * schema names. Unbound metadata sessions continue to observe live namespaces.
     * Explicit table enumeration honors DDL invalidation.</p>
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
                session.getQueryAuthority().map(org.pangrova.connector.api.query.ConnectorQueryAuthority::queryId)
                        .orElseGet(() -> session.getProperties().get("queryId")), session.getQueryAuthority());
        AbstractSchema root = new NamespaceSchema(session, () -> metadataService.listCatalogs(metadataSession),
                catalog -> catalogSchema(session, metadataSession, catalog, readBindings));
        return CalciteSchema.createRootSchema(false, false, "", root);
    }

    /** The metadata root contains only catalogs, with tables two namespace levels below it. */
    public static boolean hasCatalogNamespaces(CalciteSchema root) {
        return root.isRoot() && root.schema instanceof NamespaceSchema;
    }

    private AbstractSchema catalogSchema(ConnectorSession session, MetadataSessionContext metadataSession,
                                 String catalog, PlanningReadBindings readBindings) {
        return new NamespaceSchema(session, () -> readBindings == null
                        ? metadataService.listSchemas(metadataSession, catalog) : readBindings.listSchemaNames(catalog),
                name -> new LazyMetadataSchema(catalog, name, readBindings));
    }

    private static final class NamespaceSchema extends AbstractSchema {
        private final boolean queryScoped;
        private final Supplier<List<String>> names;
        private final Function<String, Schema> createChild;
        private final LazyReference<Map<String, Schema>> visible = new LazyReference<>();
        private final Map<String, Schema> instances = new ConcurrentHashMap<>();

        private NamespaceSchema(ConnectorSession session, Supplier<List<String>> names,
                Function<String, Schema> createChild) {
            this.queryScoped = session.getQueryAuthority().isPresent();
            this.names = names;
            this.createChild = createChild;
        }

        @Override
        protected Map<String, Schema> getSubSchemaMap() {
            return queryScoped ? visible.getOrCompute(this::load) : load();
        }

        private Map<String, Schema> load() {
            Map<String, Schema> selected = new LinkedHashMap<>();
            for (String name : names.get()) {
                selected.put(name, instances.computeIfAbsent(name, createChild));
            }
            return java.util.Collections.unmodifiableMap(selected);
        }
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
