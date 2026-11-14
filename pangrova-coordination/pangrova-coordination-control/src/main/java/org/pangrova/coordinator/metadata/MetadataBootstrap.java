package org.pangrova.coordinator.metadata;

import org.pangrova.persistence.table.api.provider.TableStores;

import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;
import org.pangrova.planner.metadata.catalog.PlanningReadBindings;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.catalog.FederationHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.governance.runtime.classification.ColumnTagService;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.metadata.outbox.MetadataSyncService;
import org.apache.calcite.jdbc.CalciteSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.persistence.materialization.api.publication.MaterializationCatalog;
import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;

/**
 * Encapsulates the metadata subsystem initialization sequence for Coordinator startup.
 * Creates CatalogRegistry, MetadataService, TieredMetadataCache, and Calcite schema bridge.
 */
public final class MetadataBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataBootstrap.class);

    private final CatalogRegistry catalogRegistry;
    private final DefaultMetadataService metadataService;
    private final TieredMetadataCache metadataCache;
    private final MetadataEventDispatcher eventDispatcher;
    private final MetadataSyncService syncService;
    private final org.pangrova.metadata.outbox.TableVersionObserver versionObserver;
    private final MaterializationRepository materializationRepository;
    private final MaterializationCatalog materializationCatalog;

    private MetadataBootstrap(
            CatalogRegistry catalogRegistry,
            DefaultMetadataService metadataService,
            TieredMetadataCache metadataCache,
            MetadataEventDispatcher eventDispatcher,
            MetadataSyncService syncService,
            org.pangrova.metadata.outbox.TableVersionObserver versionObserver,
            MaterializationRepository materializationRepository, MaterializationCatalog materializationCatalog) {
        this.catalogRegistry = catalogRegistry;
        this.metadataService = metadataService;
        this.metadataCache = metadataCache;
        this.eventDispatcher = eventDispatcher;
        this.syncService = syncService;
        this.versionObserver = versionObserver;
        this.materializationRepository = materializationRepository;
        this.materializationCatalog = materializationCatalog;
    }

    /**
     * Initializes the full metadata subsystem from a CatalogDeploymentRepository.
     * Sequence: store → registry → cache → service → initialize catalogs.
     */
    public static MetadataBootstrap initialize(TableStores stores, CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization) {
        return initialize(stores, deployments, tables, projections, synchronization, null);
    }

    /**
     * Initializes the metadata subsystem with connector support.
     * When connectorHandleFactory is provided, CONNECTOR-type catalogs can be loaded.
     */
    public static MetadataBootstrap initialize(TableStores stores, CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory) {
        return initialize(stores, deployments, tables, projections, synchronization, connectorHandleFactory, null);
    }

    /**
     * Initializes the metadata subsystem with connector and federation support.
     * CONNECTOR-type catalogs use connectorHandleFactory (PluginConnectorHandleFactory).
     * FEDERATION-type catalogs use federationHandleFactory (PluginFederationHandleFactory).
     */
    public static MetadataBootstrap initialize(
            TableStores stores, CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory) {
        return initialize(stores, deployments, tables, projections, synchronization,
                connectorHandleFactory, federationHandleFactory, null, null);
    }

    /** Initializes durable Dynamic Table publications when the metadata JDBC authority is present. */
    public static MetadataBootstrap initialize(
            TableStores stores, CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            MaterializationRepository materializationRepository, MaterializationCatalog materializationCatalog) {
        if ((materializationRepository == null) != (materializationCatalog == null)) {
            throw new IllegalArgumentException("Materialization requires both the repository and the published catalog");
        }
        MetadataEventDispatcher dispatcher = new MetadataEventDispatcher();
        CatalogRegistry registry = new CatalogRegistry(
                stores, deployments, connectorHandleFactory, federationHandleFactory, dispatcher);
        return initialize(registry, dispatcher, tables, projections, synchronization,
                materializationRepository, materializationCatalog);
    }

    static MetadataBootstrap initialize(CatalogRegistry registry, MetadataEventDispatcher dispatcher,
            TableProjectionRepository tables, MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization, MaterializationRepository materializationRepository,
            MaterializationCatalog materializationCatalog) {
        TieredMetadataCache cache = new TieredMetadataCache();
        dispatcher.addListener(cache);
        SchemaEvolutionValidator validator = new SchemaEvolutionValidator();

        DefaultMetadataService service = new DefaultMetadataService(
                registry, cache, dispatcher, validator);

        MetadataSyncService syncService = new MetadataSyncService(
                tables, projections, synchronization, cache, dispatcher);
        org.pangrova.metadata.outbox.TableVersionObserver versionObserver =
                new org.pangrova.metadata.outbox.TableVersionObserver(registry, dispatcher);

        registry.initialize();
        LOG.info("event=metadata_subsystem_initialized");

        return new MetadataBootstrap(registry, service, cache, dispatcher, syncService,
                versionObserver, materializationRepository, materializationCatalog);
    }

    /**
     * Builds a CalciteSchema bridging MetadataService to Calcite's table resolution.
     * Passes CatalogRegistry so LazyMetadataSchema can resolve tables from connectors.
     */
    public CalciteSchema buildCalciteSchema(org.pangrova.connector.api.session.ConnectorSession session) {
        return new MetadataSchemaBuilder(metadataService, catalogRegistry).buildRootSchema(session);
    }

    /** Shares the selected source owner with the session planner's statistics provider. */
    public CalciteSchema buildCalciteSchema(org.pangrova.connector.api.session.ConnectorSession session,
            PlanningReadBindings readBindings) {
        return new MetadataSchemaBuilder(metadataService, catalogRegistry).buildRootSchema(session, readBindings);
    }

    /** Subscribes to metadata lifecycle events such as observed external commits. */
    public void addMetadataListener(org.pangrova.metadata.api.event.MetadataEventListener listener) {
        eventDispatcher.addListener(listener);
    }

    public CatalogRegistry catalogRegistry() {
        return catalogRegistry;
    }

    public ColumnTagService columnTagEvents() {
        return metadataService.columnTagEvents();
    }

    public int replayColumnTags() {
        return metadataService.replayColumnTags();
    }

    public MetadataService metadataService() {
        return metadataService;
    }

    public MetadataSyncService syncService() {
        return syncService;
    }

    /** Version observation for LINKED/MANAGED tables, driven by the leader scheduler. */
    public org.pangrova.metadata.outbox.TableVersionObserver versionObserver() {
        return versionObserver;
    }

    public org.pangrova.metadata.cache.TieredMetadataCache metadataCache() {
        return metadataCache;
    }

    /** Returns the configured materialization repository, or null in metadata-only mode. */
    public MaterializationRepository materializationRepository() {
        return materializationRepository;
    }

    /** Returns the configured publication read model, or null in metadata-only mode. */
    public MaterializationCatalog materializationCatalog() {
        return materializationCatalog;
    }

    /**
     * Shuts down the metadata subsystem: registry health monitor and event dispatcher.
     */
    public void shutdown() {
        catalogRegistry.shutdown();
        eventDispatcher.shutdown();
        LOG.info("event=metadata_subsystem_shutdown");
    }
}
