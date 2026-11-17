package org.pangrova.metadata.catalog;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.metadata.api.resolution.ResolvedTable;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.DefaultConnectorSession;
import org.pangrova.metadata.ddl.CatalogTableDdl;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.governance.CatalogColumnTags;
import org.pangrova.metadata.query.MetadataQueryScopes;
import org.pangrova.metadata.resolution.CatalogTableResolution;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Implements the Metadata API for the currently installed catalog registry. */
public final class CatalogMetadata implements MetadataService {

    private final CatalogRegistry catalogRegistry;
    private final CatalogTableResolution resolution;
    private final CatalogTableDdl tableDdl;
    private final MetadataQueryScopes queryScopes;
    private final CatalogColumnTags tags;

    public CatalogMetadata(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator) {
        this(catalogRegistry, new CatalogTableResolution(catalogRegistry, cache), eventDispatcher, schemaValidator);
    }

    /** Creates the service with custom resolution-cache size and TTL bounds. */
    public CatalogMetadata(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator,
            long resolutionCacheMaxSize,
            Duration resolutionCacheTtl) {
        this(catalogRegistry, new CatalogTableResolution(catalogRegistry, cache, resolutionCacheMaxSize,
                resolutionCacheTtl),
                eventDispatcher, schemaValidator);
    }

    private CatalogMetadata(CatalogRegistry catalogRegistry, CatalogTableResolution resolution,
            MetadataEventDispatcher eventDispatcher, SchemaEvolutionValidator schemaValidator) {
        this.catalogRegistry = catalogRegistry;
        this.tags = new CatalogColumnTags(catalogRegistry);
        this.queryScopes = new MetadataQueryScopes(catalogRegistry);
        this.resolution = resolution;
        this.tableDdl = new CatalogTableDdl(catalogRegistry, resolution, eventDispatcher, schemaValidator);
    }

    public CatalogColumnTags columnTags() {
        return tags;
    }

    public MetadataQueryScopes queryScopes() {
        return queryScopes;
    }

    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        tags.setTag(session, tableRef, columnName, tagKey, value);
    }

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        tags.unsetTag(session, tableRef, columnName, tagKey);
    }

    @Override
    public Map<String, String> listTags(MetadataSessionContext session, QualifiedTableRef tableRef, String columnName) {
        return tags.listTags(session, tableRef, columnName);
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, java.util.UUID tableId) {
        return tags.clearTableTags(tableId);
    }

    @Override
    public int renameTableTags(MetadataSessionContext session, QualifiedTableRef from, QualifiedTableRef to) {
        return tags.renameTableTags(from, to);
    }

    @Override
    public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) {
        catalogRegistry.register(descriptor);
    }

    @Override
    public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) {
        catalogRegistry.unregister(catalogName);
        invalidateCatalogCaches(catalogName);
    }

    @Override
    public List<String> listCatalogs(MetadataSessionContext session) {
        return catalogRegistry.listCatalogNames();
    }

    @Override
    public java.util.Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName) {
        return catalogRegistry.getCatalogDescriptor(catalogName);
    }

    @Override
    public java.util.Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision) {
        return catalogRegistry.findCatalogDescriptorRevision(catalogName, incarnationId, revision);
    }

    @Override
    public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(session, catalogName, null);
        ConnectorMetadata metadata = handle.getMetadata(connSession);
        return metadata.listSchemaNames(connSession);
    }

    @Override
    public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        return resolution.resolveTable(session, tableRef);
    }

    @Override
    public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
        tableDdl.createTable(session, tableRef, metadata);
    }

    @Override
    public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        tableDdl.dropTable(session, tableRef);
    }

    @Override
    public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        tableDdl.alterTable(session, tableRef, changes);
    }

    public void invalidateTableCaches(QualifiedTableRef tableRef) {
        resolution.invalidateTableCaches(tableRef);
    }

    public void invalidateCatalogCaches(String catalogName) {
        resolution.invalidateCatalogCaches(catalogName);
    }

    @Override
    public void beginQuery(MetadataSessionContext session) {
        queryScopes.begin(session);
    }

    @Override
    public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
        queryScopes.cleanup(authority);
    }
}
