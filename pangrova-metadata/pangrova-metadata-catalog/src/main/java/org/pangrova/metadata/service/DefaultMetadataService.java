package org.pangrova.metadata.service;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.governance.runtime.classification.ColumnTagBindings;
import org.pangrova.governance.runtime.classification.ColumnTagService;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.metadata.api.resolution.ResolvedTable;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.DefaultConnectorSession;
import org.pangrova.metadata.ddl.CatalogTableDdl;
import org.pangrova.metadata.resolution.CatalogTableResolution;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.table.api.identity.TableRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default metadata service: catalog lifecycle, table resolution with caching,
 * schema evolution, and query-level metadata transaction management.
 *
 * <p>Table resolution results are cached in a TTL-based local cache (default 5 minutes)
 * to avoid repeated JDBC round-trips. Cache entries are invalidated on DDL operations
 * (CREATE/ALTER/DROP TABLE) and on catalog lifecycle events.</p>
 *
 * <p>Query-level transactions ensure that all metadata reads within a single
 * query see a consistent snapshot. {@link #beginQuery(MetadataSessionContext)} delegates to
 * each authenticated connector instance, which is retained with the exact issued lifetime
 * until cleanup succeeds. Completed begin and cleanup operations are idempotent.</p>
 *
 * <p>Partial catalog failures are aggregated into {@link QueryLifecycleReport}
 * and exposed via metrics counters. {@code beginQuery} throws on partial failure;
 * failed cleanup remains owned and throws so the actual lifecycle owner can retry.</p>
 */
public class DefaultMetadataService implements MetadataService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    private static final long DEFAULT_RESOLUTION_CACHE_MAX_SIZE = 10_000;
    private static final Duration DEFAULT_RESOLUTION_CACHE_TTL = Duration.ofMinutes(5);
    private final CatalogRegistry catalogRegistry;
    private final CatalogTableResolution resolution;
    private final CatalogTableDdl tableDdl;
    private final MetadataQueryLifecycles queryLifecycles;
    private final ColumnTagBindings tagOperations;

    private final AtomicLong beginQuerySuccessTotal = new AtomicLong();
    private final AtomicLong beginQueryFailureTotal = new AtomicLong();
    private final AtomicLong cleanupQuerySuccessTotal = new AtomicLong();
    private final AtomicLong cleanupQueryFailureTotal = new AtomicLong();

    public DefaultMetadataService(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator) {
        this(catalogRegistry, cache, eventDispatcher, schemaValidator,
                DEFAULT_RESOLUTION_CACHE_MAX_SIZE, DEFAULT_RESOLUTION_CACHE_TTL);
    }

    /** Creates the service with custom resolution-cache size and TTL bounds. */
    public DefaultMetadataService(
            CatalogRegistry catalogRegistry,
            MetadataCache cache,
            MetadataEventDispatcher eventDispatcher,
            SchemaEvolutionValidator schemaValidator,
            long resolutionCacheMaxSize,
            Duration resolutionCacheTtl) {
        this.catalogRegistry = catalogRegistry;
        this.tagOperations = new ColumnTagBindings(catalogRegistry.tableGovernance());
        this.queryLifecycles = new MetadataQueryLifecycles(catalogRegistry);
        this.resolution = new CatalogTableResolution(catalogRegistry, cache,
                resolutionCacheMaxSize, resolutionCacheTtl);
        this.tableDdl = new CatalogTableDdl(catalogRegistry, resolution, eventDispatcher, schemaValidator);
    }

    public ColumnTagService columnTagEvents() {
        return tagOperations.events();
    }

    /**
     * Re-emits every persisted tag after wiring the governance tag service, so
     * derived policy (for example auto-masks) is rebuilt after a restart.
     *
     * @return the number of tag entries replayed
     */
    public int replayColumnTags() {
        return tagOperations.replayAll();
    }

    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        target.requireTagType(tagKey);
        tagOperations.setTag(target.owner(), target.column(), tagKey, value, session.user());
    }

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        tagOperations.unsetTag(target.owner(), target.column(), tagKey);
    }

    @Override
    public Map<String, String> listTags(MetadataSessionContext session,
                                        QualifiedTableRef tableRef, String columnName) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        return tagOperations.listTags(target.owner(), target.column());
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, java.util.UUID tableId) {
        return tagOperations.clearTable(tableId);
    }

    @Override
    public int renameTableTags(MetadataSessionContext session, QualifiedTableRef from,
            QualifiedTableRef to) {
        return tagOperations.renameTable(new TableRef(from.catalog(), from.schema(), from.table()),
                new TableRef(to.catalog(), to.schema(), to.table()));
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
        QueryLifecycleReport report = queryLifecycles.begin(session);
        recordBeginQueryMetrics(report);
        if (report.hasFailures()) {
            throw new MetadataResolutionException(session.queryId(), "beginQuery", report.failedCatalogs());
        }
        LOG.debug("beginQuery completed for queryId={}", session.queryId());
    }

    @Override
    public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
        QueryLifecycleReport report = queryLifecycles.cleanup(authority);
        recordCleanupQueryMetrics(report);
        if (report.hasFailures()) {
            throw new MetadataResolutionException(authority.queryId(), "cleanupQuery", report.failedCatalogs());
        }
        LOG.debug("cleanupQuery completed for queryId={}", authority.queryId());
    }

    public long beginQuerySuccessTotal() {
        return beginQuerySuccessTotal.get();
    }

    public long beginQueryFailureTotal() {
        return beginQueryFailureTotal.get();
    }

    public long cleanupQuerySuccessTotal() {
        return cleanupQuerySuccessTotal.get();
    }

    public long cleanupQueryFailureTotal() {
        return cleanupQueryFailureTotal.get();
    }

    public void onQueryStarted(String queryId, String sql) {
        LOG.debug("event=query_lifecycle_started queryId={}", queryId);
    }

    public void onQueryCompleted(String queryId) {
        onQueryEnd(queryId);
        LOG.debug("event=query_lifecycle_completed queryId={}", queryId);
    }

    public void onQueryFailed(String queryId, Throwable error) {
        onQueryEnd(queryId);
        LOG.debug("event=query_lifecycle_failed queryId={}", queryId);
    }

    /**
     * Looks up the actual retained lifetime; a listener cannot issue a new query authority.
     */
    public void onQueryEnd(String queryId) {
        var authority = queryLifecycles.retainedAuthority(queryId);
        if (authority != null) cleanupQuery(authority);
    }

    /**
     * Returns the number of currently tracked active queries.
     * Exposed for monitoring and testing purposes.
     */
    public int activeQueryCount() {
        return queryLifecycles.size();
    }

    private void recordBeginQueryMetrics(QueryLifecycleReport report) {
        beginQuerySuccessTotal.addAndGet(report.succeededCount());
        beginQueryFailureTotal.addAndGet(report.failedCount());
    }

    private void recordCleanupQueryMetrics(QueryLifecycleReport report) {
        cleanupQuerySuccessTotal.addAndGet(report.succeededCount());
        cleanupQueryFailureTotal.addAndGet(report.failedCount());
    }

}
