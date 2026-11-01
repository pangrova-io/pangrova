package org.pangrova.metadata.catalog.registry;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.exception.catalog.CatalogNotFoundException;
import org.pangrova.metadata.exception.catalog.CatalogProviderNotFoundException;
import org.pangrova.table.api.control.CatalogDeploymentIdentity;
import org.pangrova.table.store.memory.control.InMemoryTableControlStatisticsStore;
import org.pangrova.table.store.memory.control.InMemoryTableControlStore;
import org.pangrova.table.control.TableControlService;
import org.pangrova.table.statistics.TableControlStatisticsService;
import org.pangrova.metadata.exception.catalog.CatalogSuspendedException;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.metadata.catalog.handle.federation.FederationHandleFactory;
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
/*
 * Package governance review (2026-03): catalog/ has 12 files (review zone 9-15).
 * Two naming clusters exist: Catalog* (Registry, Descriptor, State, Type, Handle,
 * HealthMonitor, HealthStatus) and *HandleFactory + *CatalogHandle (Connector,
 * Federation, Internal). Splitting into catalog.handle/ was considered but rejected:
 * tight coupling between Handle interfaces and their factories would create a
 * 2-file fragment package. Re-evaluate on next file addition.
 */
@ThreadSafe
public class CatalogRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogRegistry.class);

    // GuardedBy("this") for compound operations; ConcurrentHashMap for safe concurrent reads
    private final Map<String, CatalogHandle> catalogs = new ConcurrentHashMap<>();
    // Names currently being registered -- guards the window between check and loadCatalog
    private final java.util.Set<String> registering = ConcurrentHashMap.newKeySet();
    private final MetadataStore store;
    private final CatalogDeploymentResolver catalogIdentities;
    private final CatalogHealthMonitor healthMonitor;
    private final ConnectorHandleFactory connectorHandleFactory;
    private final FederationHandleFactory federationHandleFactory;
    private final MetadataEventDispatcher eventDispatcher;
    // Ownership and write admission share this authority with DDL and resolution.
    private volatile TableControlService tableControl;
    private volatile org.pangrova.table.api.governancebinding.TableControlGovernanceBindingStore governanceBindings =
            new org.pangrova.table.store.memory.control.InMemoryTableControlGovernanceBindingStore();
    // ANALYZE results bound to a controlled table's stable ID and version token.
    private volatile TableControlStatisticsService tableStatistics;

    /**
     * Creates a CatalogRegistry with all dependencies injected.
     *
     * @param store                    persistent catalog descriptor store (required)
     * @param connectorHandleFactory   factory for CONNECTOR-type catalogs, or null if unsupported
     * @param federationHandleFactory  factory for FEDERATION-type catalogs, or null if unsupported
     * @param eventDispatcher          event dispatcher for catalog lifecycle events, or null to skip events
     */
    public CatalogRegistry(
            MetadataStore store,
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
                new InMemoryTableControlStore(),
                catalogIdentities,
                org.pangrova.table.control.TableControlSettings.fromSystemProperties());
        this.tableStatistics = new TableControlStatisticsService(
                this.tableControl, new InMemoryTableControlStatisticsStore());
    }

    /**
     * Convenience constructor for minimal setups (e.g. tests) that only need
     * INTERNAL catalogs and no event dispatching.
     *
     * @param store persistent catalog descriptor store
     */
    public CatalogRegistry(MetadataStore store) {
        this(store, null, null, null);
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
            loadCatalog(descriptor);
        } catch (RuntimeException e) {
            compensateFailedRegistration(descriptor.name(), e);
            throw e;
        } finally {
            registering.remove(descriptor.name());
        }
        dispatchEvent(new MetadataEvent.CatalogCreatedEvent(descriptor));
        LOG.info("Registered catalog, catalog={}", descriptor.name());
    }

    private void compensateFailedRegistration(String name, RuntimeException cause) {
        CatalogHandle leakedHandle = catalogs.remove(name);
        if (leakedHandle != null) {
            leakedHandle.shutdown();
        }
        tryDeleteDescriptor(name, cause);
    }

    /**
     * Unregisters a catalog. The map removal is synchronized; shutdown and store
     * deletion run outside the lock to avoid blocking concurrent reads.
     */
    public void unregister(String name) {
        CatalogHandle handle;
        synchronized (this) {
            handle = catalogs.remove(name);
            if (handle == null) {
                throw new CatalogNotFoundException("Catalog '" + name + "' not found");
            }
        }
        handle.shutdown();
        store.deleteCatalogDescriptor(name);
        dispatchEvent(new MetadataEvent.CatalogDroppedEvent(name));
        LOG.info("Unregistered catalog, catalog={}", name);
    }

    /** Looks up a catalog by name, failing if missing or suspended. */
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

    public List<String> listCatalogNames() {
        return List.copyOf(catalogs.keySet());
    }

    /** Resolves the deployment authority and rejects a stale local connector handle. */
    public CatalogDeploymentIdentity requireCurrentCatalogIdentity(String name) {
        return catalogIdentities.apply(name);
    }

    /** Ownership and write-admission authority for tables in every catalog. */
    public TableControlService tableControl() {
        return tableControl;
    }

    /** Shares the stable-identity governance authority with metadata DDL and replay. */
    public org.pangrova.table.control.TableControlPolicyBinder tableGovernance() {
        return new org.pangrova.table.control.TableControlPolicyBinder(tableControl, governanceBindings);
    }

    /** Installs the durable governance authority before serving requests. */
    public void setGovernanceBindingStore(
            org.pangrova.table.api.governancebinding.TableControlGovernanceBindingStore bindings) {
        governanceBindings = java.util.Objects.requireNonNull(bindings, "bindings");
    }

    /** Statistics collected for controlled tables, keyed by stable table ID. */
    public TableControlStatisticsService tableStatistics() {
        return tableStatistics;
    }

    /**
     * Replaces the control authority, typically with a JDBC-backed store at bootstrap.
     * The statistics service is rebuilt because it reads ownership through this
     * authority; bootstrap replaces it again with the durable store when available.
     */
    public void setTableControlService(TableControlService service) {
        this.tableControl = java.util.Objects.requireNonNull(service, "service");
        this.tableStatistics = new TableControlStatisticsService(
                service, new InMemoryTableControlStatisticsStore());
    }

    /** Replaces the statistics authority, typically with a JDBC-backed store. */
    public void setTableStatisticsService(TableControlStatisticsService service) {
        this.tableStatistics = java.util.Objects.requireNonNull(service, "service");
    }

    /** Historical deployments are read from the durable store, not the live handle cache. */
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        return store.findCatalogDescriptorRevision(name, incarnationId, revision);
    }

    /** Shuts down the health monitor and all registered catalogs. */
    public void shutdown() {
        healthMonitor.stop();
        catalogs.values().forEach(CatalogHandle::shutdown);
        catalogs.clear();
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

        loadMissingCatalogs(storeByName, localNames, loaded, loadFailed);
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

    /** Loads catalogs present in the store but missing from the local registry. */
    private void loadMissingCatalogs(Map<String, CatalogDescriptor> storeByName,
                                     Set<String> localNames,
                                     List<String> loaded, List<String> loadFailed) {
        for (var descriptor : storeByName.entrySet()) {
            if (!localNames.contains(descriptor.getKey())) {
                loadCatalogForReconcile(descriptor.getKey(), descriptor.getValue(), loaded, loadFailed);
            }
        }
    }

    /** Loads a single peer-added catalog, isolating load failures from the rest. */
    private void loadCatalogForReconcile(String name, CatalogDescriptor descriptor,
                                         List<String> loaded, List<String> loadFailed) {
        try {
            loadCatalog(descriptor);
            loaded.add(name);
            LOG.info("event=catalog_reconcile_loaded catalog={}", name);
        } catch (Exception e) {
            loadFailed.add(name);
            LOG.warn("event=catalog_reconcile_load_failed catalog={} error={}",
                    name, e.getMessage(), e);
        }
    }

    /** Removes catalogs present locally but missing from the store. */
    private void removeObsoleteCatalogs(Set<String> storeNames, Set<String> localNames,
                                        List<String> removed) {
        for (String localName : localNames) {
            if (!storeNames.contains(localName)) {
                CatalogHandle handle = catalogs.remove(localName);
                if (handle != null) {
                    handle.shutdown();
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
        private final MetadataStore store;
        private final Map<String, CatalogHandle> handles;

        CatalogDeploymentResolver(MetadataStore store, Map<String, CatalogHandle> handles) {
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
                throw new org.pangrova.spi.security.AccessDeniedException(
                        "Catalog deployment changed for " + name + "; reload the local catalog before admission");
            }
            return new CatalogDeploymentIdentity(current.incarnationId(), current.revision());
        }
    }

    private void loadCatalog(CatalogDescriptor descriptor) {
        CatalogHandle handle = createHandle(descriptor);
        catalogs.put(descriptor.name(), handle);
    }

    private CatalogHandle createHandle(CatalogDescriptor descriptor) {
        return switch (descriptor.type()) {
            case INTERNAL -> new InternalCatalogHandle(descriptor);
            case CONNECTOR -> createConnectorHandle(descriptor);
            case FEDERATION -> createFederationHandle(descriptor);
        };
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

    private void tryDeleteDescriptor(String name, RuntimeException originalError) {
        try {
            store.deleteCatalogDescriptor(name);
        } catch (RuntimeException cleanupError) {
            // F-META-002: preserve the causal chain — attach the cleanup failure as a suppressed
            // exception on the original error (which register() re-throws), so callers see both
            // the registration failure and that compensation cleanup also failed.
            originalError.addSuppressed(cleanupError);
            LOG.error("event=catalog_registration_cleanup_failed catalog={} originalError={} cleanupError={}",
                    name, originalError.getMessage(), cleanupError.getMessage(), cleanupError);
        }
    }
}
