package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Bridges MetadataService and CatalogRegistry to Calcite's schema mechanism.
 * Creates a CalciteSchema tree from registered catalogs so Calcite validator
 * can resolve table references during SQL compilation.
 */
public final class MetadataSchemaBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataSchemaBuilder.class);

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
     * every table lookup delegates to {@link LazyMetadataSchema#getTableMap()} which
     * checks the DDL generation counter and refreshes when needed.</p>
     */
    public CalciteSchema buildRootSchema() {
        SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
        MetadataSessionContext session = buildSystemSession();
        registerCatalogs(rootSchema, session);
        return CalciteSchema.from(rootSchema);
    }

    private void registerCatalogs(SchemaPlus root, MetadataSessionContext session) {
        List<String> catalogs = metadataService.listCatalogs(session);
        for (String catalog : catalogs) {
            registerCatalogSchemas(root, session, catalog);
        }
        LOG.info("event=calcite_schema_registered catalogCount={}", catalogs.size());
    }

    private void registerCatalogSchemas(SchemaPlus root, MetadataSessionContext session, String catalog) {
        try {
            List<String> schemas = metadataService.listSchemas(session, catalog);
            SchemaPlus catalogSchema = root.add(catalog, new AbstractSchema() {});
            for (String schema : schemas) {
                catalogSchema.add(schema, new LazyMetadataSchema(catalogRegistry, catalog, schema));
            }
        } catch (Exception e) {
            LOG.warn("event=catalog_schema_load_failed catalog={} error={}",
                    catalog, e.getMessage());
            root.add(catalog, new AbstractSchema() {});
        }
    }

    private static MetadataSessionContext buildSystemSession() {
        return new MetadataSessionContext(
                "system", null, null, null, null, Map.of(), null, null, null);
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
