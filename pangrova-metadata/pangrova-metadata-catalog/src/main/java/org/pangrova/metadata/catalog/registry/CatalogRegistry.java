package org.pangrova.metadata.catalog.registry;

import org.pangrova.metadata.api.exception.CatalogAccessDeniedException;
import org.pangrova.connector.api.index.CommitListener;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorMetadataLease;
import org.pangrova.connector.api.metadata.ConnectorMetadataScope;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.catalog.CatalogReadAccess;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.exception.CatalogNotFoundException;
import org.pangrova.metadata.api.exception.CatalogBackendException;
import org.pangrova.metadata.api.exception.CatalogProviderNotFoundException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.persistence.table.api.provider.TableStores;
import org.pangrova.table.maintenance.TableControlMaintenanceService;
import org.pangrova.table.policy.TableControlPolicyBinder;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.statistics.TableControlStatisticsService;
import org.pangrova.metadata.api.exception.CatalogSuspendedException;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.catalog.FederationHandleFactory;
import org.pangrova.metadata.catalog.handle.internal.InternalCatalogHandle;

/**
 * Central registry for catalog lifecycle: register, unregister, lookup, health monitoring.
 *
 * <p>Thread safety: all mutable state is guarded by {@code synchronized} blocks on the
 * registry lock to ensure atomicity of compound check-then-act operations. The underlying
 * {@link ConcurrentHashMap} provides safe concurrent reads for {@link #getCatalog} and
 * {@link #listCatalogNames} without locking.</p>
 *
 * <p>All dependencies are injected via the constructor. Optional factories may be null
 * when the corresponding catalog type is not supported in the deployment.</p>
 */
@ThreadSafe
public class CatalogRegistry implements CatalogReadAccess {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogRegistry.class);

    // GuardedBy("this") for compound operations; ConcurrentHashMap for safe concurrent reads
    private final Map<String, CatalogHandle> catalogs = new ConcurrentHashMap<>();
    // Names currently being registered -- guards the window between check and loadCatalog
    private final java.util.Set<String> registering = ConcurrentHashMap.newKeySet();
    // A querying authority can retain metadata from this particular handle after a revision change.
    private final Map<CatalogHandle, Integer> queryLeases = new IdentityHashMap<>();
    // Listeners registered on an incarnation follow its revisions, never a different deployment.
    private final Map<String, List<CommitListener>> commitListeners = new HashMap<>();
    private final Set<CatalogHandle> retired = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final CatalogDeploymentRepository store;
    private final CatalogDeploymentResolver catalogIdentities;
    private final CatalogHealthMonitor healthMonitor;
    private final ConnectorHandleFactory connectorHandleFactory;
    private final FederationHandleFactory federationHandleFactory;
    private final MetadataEventDispatcher eventDispatcher;
    // Ownership and write admission share this authority with DDL and resolution.
    private final TableControlService tableControl;
    private final TableControlPolicyBinder governance;
    private final TableControlMaintenanceService tableMaintenance;
    // ANALYZE results bound to a controlled table's stable ID and version token.
    private final TableControlStatisticsService tableStatistics;

    /**
     * Creates a CatalogRegistry with all dependencies injected.
     *
     * @param stores                   selected table storage authority
     * @param store                    persistent catalog descriptor store (required)
     * @param connectorHandleFactory   factory for CONNECTOR-type catalogs, or null if unsupported
     * @param federationHandleFactory  factory for FEDERATION-type catalogs, or null if unsupported
     * @param eventDispatcher          event dispatcher for catalog lifecycle events, or null to skip events
     */
    public CatalogRegistry(
            TableStores stores,
            CatalogDeploymentRepository store,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            MetadataEventDispatcher eventDispatcher) {
        this.store = store;
        this.catalogIdentities = new CatalogDeploymentResolver(store, catalogs);
        this.connectorHandleFactory = connectorHandleFactory;
        this.federationHandleFactory = federationHandleFactory;
        this.eventDispatcher = eventDispatcher;
        this.healthMonitor = new CatalogHealthMonitor(catalogs);
        this.tableControl = new TableControlService(
                stores.control(),
                catalogIdentities,
                org.pangrova.table.config.TableControlSettings.fromSystemProperties());
        this.tableStatistics = new TableControlStatisticsService(this.tableControl, stores.statistics());
        this.governance = new TableControlPolicyBinder(tableControl, stores.governance());
        this.tableMaintenance = new TableControlMaintenanceService(stores.maintenance());
    }

    /**
     * Convenience constructor for minimal setups (e.g. tests) that only need
     * INTERNAL catalogs and no event dispatching.
     *
     * @param store persistent catalog descriptor store
     */
    public CatalogRegistry(TableStores stores, CatalogDeploymentRepository store) {
        this(stores, store, null, null, null);
    }

    public void initialize() {
        store.listAllCatalogDescriptors().forEach(this::loadCatalog);
        healthMonitor.start();
        LOG.info("CatalogRegistry initialized, catalogCount={}", catalogs.size());
    }

    /**
     * Registers a catalog. The check-then-act on the map is synchronized;
     * I/O (persist + load) runs outside the lock to avoid blocking concurrent reads.
     */
    public void register(CatalogDescriptor descriptor) {
        synchronized (this) {
            if (catalogs.containsKey(descriptor.name()) || !registering.add(descriptor.name())) {
                LOG.info("Catalog already registered, catalog={}, action=skip", descriptor.name());
                return;
            }
        }
        try {
            store.saveCatalogDescriptor(descriptor);
            try {
                loadCatalog(descriptor);
            } catch (RuntimeException e) {
                // The save was acknowledged. A later deployment must survive failed handle creation.
                tryDeleteDescriptor(descriptor, e);
                throw e;
            }
        } finally {
            registering.remove(descriptor.name());
        }
        dispatchEvent(new MetadataEvent.CatalogCreatedEvent(descriptor));
        LOG.info("Registered catalog, catalog={}", descriptor.name());
    }

    /** Resolves current durable deployment under its tenant admission policy. */
    public CatalogDescriptor requireVisibleDescriptor(String name, String tenant) {
        CatalogDescriptor descriptor = store.getCatalogDescriptor(name)
                .orElseThrow(() -> new CatalogNotFoundException("Catalog '" + name + "' not found"));
        if (!descriptor.allowedTenants().isEmpty()
                && (tenant == null || !descriptor.allowedTenants().contains(tenant))) {
            throw new CatalogAccessDeniedException("Catalog '" + name + "' not found");
        }
        return descriptor;
    }

    /**
     * Validates a replacement handle, then atomically publishes the next revision,
     * immutable history and invalidation under the repository's predecessor fence.
     * Failed or unknown commits never compensate by deleting the durable deployment.
     */
    public synchronized void alterProperties(String name, String tenant, Map<String, String> updates) {
        CatalogDescriptor current = requireVisibleDescriptor(name, tenant);
        if (registering.contains(name)) {
            throw new CatalogBackendException("Catalog registration in progress: " + name);
        }
        if (updates.isEmpty() || updates.keySet().stream().anyMatch("connector"::equalsIgnoreCase)) {
            throw new IllegalArgumentException("ALTER CATALOG requires connector properties and cannot replace its provider");
        }
        var properties = new HashMap<>(current.properties());
        properties.putAll(updates);
        CatalogDescriptor next = new CatalogDescriptor(name, current.type(), current.provider(),
                current.state(), properties, current.comment(), current.allowedTenants(),
                current.incarnationId(), Math.incrementExact(current.revision()));
        CatalogHandle replacement = createHandle(next);
        try {
            store.saveCatalogDescriptor(next);
        } catch (RuntimeException | Error failure) {
            abandonHandle(replacement, failure);
            throw failure;
        }
        CatalogHandle previous = catalogs.put(name, replacement);
        if (previous != null) retireHandle(previous);
    }

    /** Binds tenant visibility and conditional deletion to exactly the same durable revision. */
    public synchronized void unregister(String name, String tenant) {
        var visible = requireVisibleDescriptor(name, tenant);
        var handle = catalogs.get(name);
        if (handle == null || !visible.identity().equals(handle.getDescriptor().identity())) {
            throw new CatalogBackendException("Catalog deployment changed before drop: " + name);
        }
        unregister(name);
    }

    /** Drops the deployment represented by the local handle using its immutable revision. */
    public void unregister(String name) {
        CatalogHandle handle;
        synchronized (this) {
            handle = catalogs.get(name);
            if (handle == null) {
                throw new CatalogNotFoundException("Catalog '" + name + "' not found");
            }
            if (!store.deleteCatalogDescriptor(handle.getDescriptor().identity())) {
                throw new CatalogBackendException(
                        "Catalog deployment changed before drop: " + name);
            }
            catalogs.remove(name);
            commitListeners.remove(handle.getDescriptor().incarnationId());
        }
        try {
            retireHandle(handle);
        } finally {
            dispatchEvent(new MetadataEvent.CatalogDroppedEvent(name));
        }
        LOG.info("Unregistered catalog, catalog={}", name);
    }

    /** Looks up a catalog by name, failing if missing or suspended. */
    @Override
    public CatalogHandle getCatalog(String name) {
        CatalogHandle handle = catalogs.get(name);
        if (handle == null) {
            throw new CatalogNotFoundException("Catalog '" + name + "' not found");
        }
        if (handle.getState() == CatalogState.SUSPENDED) {
            throw new CatalogSuspendedException("Catalog '" + name + "' is suspended");
        }
        return handle;
    }

    /** Binds an acknowledged event subscription to the deployment, including subsequent revisions. */
    public synchronized void registerCommitListener(String catalog, CommitListener listener) {
        var handle = getCatalog(catalog);
        String incarnation = handle.getDescriptor().incarnationId();
        var listeners = commitListeners.computeIfAbsent(incarnation, ignored -> new ArrayList<>());
        if (!listeners.contains(listener)) {
            handle.registerCommitListener(listener);
            listeners.add(listener);
        }
    }

    /** The handle and metadata selected together for one resolved catalog access. */
    public record QueryCatalog(CatalogHandle handle, ConnectorMetadata metadata) {}

    @Override
    public ConnectorMetadata metadataForQuery(String name, ConnectorSession session) {
        return queryCatalog(name, session).metadata();
    }

    /** Returns the exact data providers associated with this query's winning metadata revision. */
    public QueryCatalog queryCatalog(String name, ConnectorSession session) {
        if (session.getQueryAuthority().isEmpty()) {
            var handle = getCatalog(name);
            return new QueryCatalog(handle, handle.getMetadata(session));
        }
        var scope = ConnectorMetadataScope.require(session.getQueryAuthority().orElseThrow());
        var retained = scope.acquireMetadata(name, () -> acquireMetadata(name, session));
        if (!(retained instanceof CatalogLease lease) || lease.registry != this) {
            throw new IllegalStateException("Query metadata has a different catalog owner: " + name);
        }
        return lease.catalog;
    }

    private CatalogLease acquireMetadata(String name, ConnectorSession session) {
        var handle = retainForQuery(name);
        try {
            return new CatalogLease(this, new QueryCatalog(handle,
                    java.util.Objects.requireNonNull(handle.getMetadata(session), "catalog metadata")));
        } catch (RuntimeException | Error failure) {
            try {
                releaseFromQuery(handle);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private record CatalogLease(CatalogRegistry registry, QueryCatalog catalog) implements ConnectorMetadataLease {
        @Override public ConnectorMetadata metadata() { return catalog.metadata(); }
        @Override public void release() { registry.releaseFromQuery(catalog.handle()); }
    }

    /** Retains the exact handle until the metadata query has released its connector resources. */
    public synchronized CatalogHandle retainForQuery(String name) {
        CatalogHandle handle = getCatalog(name);
        queryLeases.merge(handle, 1, Math::addExact);
        return handle;
    }

    /** Releases one query's reference and closes an obsolete handle after its last borrower exits. */
    public synchronized void releaseFromQuery(CatalogHandle handle) {
        Integer count = queryLeases.get(handle);
        if (count == null) throw new IllegalStateException("Catalog handle has no query lease");
        if (count > 1) {
            queryLeases.put(handle, count - 1);
        } else {
            if (retired.contains(handle)) {
                handle.shutdown();
                retired.remove(handle);
            }
            queryLeases.remove(handle);
        }
    }

    private synchronized void retireHandle(CatalogHandle handle) {
        retired.add(handle);
        if (!queryLeases.containsKey(handle)) {
            handle.shutdown();
            retired.remove(handle);
        }
    }

    public List<String> listCatalogNames() {
        return List.copyOf(catalogs.keySet());
    }

    /** Resolves the deployment authority and rejects a stale local connector handle. */
    public CatalogDeploymentIdentity requireCurrentCatalogIdentity(String name) {
        return catalogIdentities.apply(name);
    }

    /** Ownership and write-admission authority for tables in every catalog. */
    @Override
    public TableControlService tableControl() {
        return tableControl;
    }

    /** Shares the stable-identity governance authority with metadata DDL and replay. */
    public TableControlPolicyBinder tableGovernance() {
        return governance;
    }

    /** Statistics collected for controlled tables, keyed by stable table ID. */
    public TableControlStatisticsService tableStatistics() {
        return tableStatistics;
    }

    /** Maintenance policies and history shared by SQL and scheduled maintenance. */
    public TableControlMaintenanceService tableMaintenance() {
        return tableMaintenance;
    }

    /** Reads the current durable catalog descriptor, independent of local handle state. */
    public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
        return store.getCatalogDescriptor(name);
    }

    /** Historical deployments are read from the durable store, not the live handle cache. */
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        return store.findCatalogDescriptorRevision(name, incarnationId, revision);
    }

    /** Shuts down the health monitor and all registered catalogs. */
    public synchronized void shutdown() {
        healthMonitor.stop();
        commitListeners.clear();
        retired.addAll(catalogs.values());
        catalogs.clear();
        for (var handle : List.copyOf(retired)) {
            retireHandle(handle);
        }
        LOG.info("CatalogRegistry shut down");
    }

    /**
     * Detects and repairs divergence between the local in-memory catalog registry
     * and the authoritative shared store (catalog_definitions).
     *
     * <p>Called at leader election time (Task 38 / G-META-02) to ensure the newly
     * promoted leader has a consistent view of all catalogs. Divergence can occur
     * when a peer coordinator adds or drops catalogs while this node was standby
     * and the invalidation-log propagation had not yet been polled.</p>
     *
     * <p>Reconciliation:
     * <ul>
     *   <li>Catalogs in the store but not in local registry are loaded (peer-added).</li>
     *   <li>Catalogs in local registry but not in the store are removed (peer-dropped).</li>
     *   <li>Load failures are isolated: a failing catalog does not block others.</li>
     * </ul></p>
     *
     * @return a report describing what was loaded, removed, and what failed
     */
    public synchronized DivergenceReport reconcileFromStore() {
        Map<String, CatalogDescriptor> storeByName = indexStoreDescriptors();
        Set<String> localNames = new HashSet<>(catalogs.keySet());

        List<String> loaded = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        List<String> loadFailed = new ArrayList<>();

        loadChangedCatalogs(storeByName, loaded, loadFailed);
        removeObsoleteCatalogs(storeByName.keySet(), localNames, removed);

        DivergenceReport report = new DivergenceReport(loaded, removed, loadFailed);

        if (!report.isEmpty()) {
            LOG.info("event=catalog_reconcile_complete loaded={} removed={} loadFailed={}",
                    loaded.size(), removed.size(), loadFailed.size());
        } else {
            LOG.debug("event=catalog_reconcile_complete divergence=none");
        }

        return report;
    }

    /** Indexes all store descriptors by catalog name. */
    private Map<String, CatalogDescriptor> indexStoreDescriptors() {
        Map<String, CatalogDescriptor> storeByName = new java.util.HashMap<>();
        for (CatalogDescriptor d : store.listAllCatalogDescriptors()) {
            storeByName.put(d.name(), d);
        }
        return storeByName;
    }

    /** Refreshes handles whose local incarnation or revision differs from current storage. */
    private void loadChangedCatalogs(Map<String, CatalogDescriptor> descriptors,
            List<String> loaded, List<String> loadFailed) {
        for (CatalogDescriptor descriptor : descriptors.values()) {
            String name = descriptor.name();
            CatalogHandle current = catalogs.get(name);
            if (registering.contains(name)
                    || (current != null && current.getDescriptor().identity().equals(descriptor.identity()))) {
                continue;
            }
            // Evict an obsolete handle even if creating its replacement fails.
            try {
                if (current != null) {
                    catalogs.remove(name);
                    if (!current.getDescriptor().incarnationId().equals(descriptor.incarnationId())) {
                        commitListeners.remove(current.getDescriptor().incarnationId());
                    }
                    retireHandle(current);
                }
                loadCatalog(descriptor);
                loaded.add(name);
                LOG.info("event=catalog_reconcile_loaded catalog={}", name);
            } catch (Exception e) {
                loadFailed.add(name);
                LOG.warn("event=catalog_reconcile_load_failed catalog={} error={}", name, e.getMessage(), e);
            }
        }
    }

    /** Removes catalogs present locally but missing from the store. */
    private void removeObsoleteCatalogs(Set<String> storeNames, Set<String> localNames,
                                        List<String> removed) {
        for (String localName : localNames) {
            if (!storeNames.contains(localName)) {
                CatalogHandle handle = catalogs.remove(localName);
                if (handle != null) {
                    commitListeners.remove(handle.getDescriptor().incarnationId());
                    retireHandle(handle);
                    removed.add(localName);
                    LOG.info("event=catalog_reconcile_removed catalog={}", localName);
                }
            }
        }
    }

    /**
     * Report of divergence found and repaired by {@link #reconcileFromStore()}.
     *
     * @param loaded     catalog names that were loaded from the store (peer-added)
     * @param removed    catalog names that were removed from local registry (peer-dropped)
     * @param loadFailed catalog names that failed to load (isolated, did not block others)
     */
    public record DivergenceReport(List<String> loaded, List<String> removed, List<String> loadFailed) {
        /** Freezes the reconciliation result independently of the producer's working lists. */
        public DivergenceReport {
            loaded = List.copyOf(loaded);
            removed = List.copyOf(removed);
            loadFailed = List.copyOf(loadFailed);
        }

        /** Returns true if no divergence was found. */
        public boolean isEmpty() {
            return loaded.isEmpty() && removed.isEmpty() && loadFailed.isEmpty();
        }
    }

    /** Reads shared deployment authority without capturing a partially constructed registry. */
    private static final class CatalogDeploymentResolver implements Function<String, CatalogDeploymentIdentity> {
        private final CatalogDeploymentRepository store;
        private final Map<String, CatalogHandle> handles;

        CatalogDeploymentResolver(CatalogDeploymentRepository store, Map<String, CatalogHandle> handles) {
            this.store = store;
            this.handles = handles;
        }

        @Override
        public CatalogDeploymentIdentity apply(String name) {
            CatalogHandle handle = handles.get(name);
            if (handle == null) {
                throw new CatalogNotFoundException("Catalog '" + name + "' not found");
            }
            if (handle.getState() == CatalogState.SUSPENDED) {
                throw new CatalogSuspendedException("Catalog '" + name + "' is suspended");
            }
            CatalogDescriptor local = handle.getDescriptor();
            CatalogDescriptor current = store.getCatalogDescriptor(name)
                    .orElseThrow(() -> new CatalogNotFoundException("Catalog '" + name + "' was removed"));
            if (!current.incarnationId().equals(local.incarnationId())
                    || current.revision() != local.revision()) {
                throw new org.pangrova.security.api.exception.AccessDeniedException(
                        "Catalog deployment changed for " + name + "; reload the local catalog before admission");
            }
            return new CatalogDeploymentIdentity(current.incarnationId(), current.revision());
        }
    }

    private void loadCatalog(CatalogDescriptor descriptor) {
        CatalogHandle handle = createHandle(descriptor);
        catalogs.put(descriptor.name(), handle);
    }

    private synchronized CatalogHandle createHandle(CatalogDescriptor descriptor) {
        var handle = switch (descriptor.type()) {
            case INTERNAL -> new InternalCatalogHandle(descriptor);
            case CONNECTOR -> createConnectorHandle(descriptor);
            case FEDERATION -> createFederationHandle(descriptor);
        };
        try {
            for (var listener : commitListeners.getOrDefault(descriptor.incarnationId(), List.of())) {
                handle.registerCommitListener(listener);
            }
            return handle;
        } catch (RuntimeException | Error failure) {
            abandonHandle(handle, failure);
            throw failure;
        }
    }

    private void abandonHandle(CatalogHandle handle, Throwable failure) {
        try {
            retireHandle(handle);
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
        }
    }

    private CatalogHandle createConnectorHandle(CatalogDescriptor descriptor) {
        if (connectorHandleFactory == null) {
            throw new CatalogProviderNotFoundException(
                    "No connector handle factory configured for '" + descriptor.name() + "'");
        }
        return connectorHandleFactory.create(descriptor);
    }

    private CatalogHandle createFederationHandle(CatalogDescriptor descriptor) {
        if (federationHandleFactory == null) {
            throw new CatalogProviderNotFoundException(
                    "No federation handle factory configured for '" + descriptor.name() + "'");
        }
        return federationHandleFactory.create(descriptor);
    }

    private void dispatchEvent(MetadataEvent event) {
        if (eventDispatcher != null) {
            eventDispatcher.dispatch(event);
        }
    }

    private void tryDeleteDescriptor(CatalogDescriptor descriptor, RuntimeException originalError) {
        try {
            store.deleteCatalogDescriptor(descriptor.identity());
        } catch (RuntimeException cleanupError) {
            // F-META-002: preserve the causal chain — attach the cleanup failure as a suppressed
            // exception on the original error (which register() re-throws), so callers see both
            // the registration failure and that compensation cleanup also failed.
            originalError.addSuppressed(cleanupError);
            LOG.error("event=catalog_registration_cleanup_failed catalog={} originalError={} cleanupError={}",
                    descriptor.name(), originalError.getMessage(), cleanupError.getMessage(), cleanupError);
        }
    }
}
