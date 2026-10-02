package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.catalog.FederationHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.mv.InMemoryMaterializedViewStore;
import org.pangrova.metadata.mv.MaterializedViewStore;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.MetadataSyncService;
import org.apache.calcite.jdbc.CalciteSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the metadata subsystem initialization sequence for Coordinator startup.
 * Creates CatalogRegistry, MetadataService, TieredMetadataCache, and Calcite schema bridge.
 */
public final class MetadataBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataBootstrap.class);

    private final CatalogRegistry catalogRegistry;
    private final MetadataService metadataService;
    private final TieredMetadataCache metadataCache;
    private final MetadataEventDispatcher eventDispatcher;
    private final MetadataSyncService syncService;
    private final MaterializedViewStore materializedViewStore;

    private MetadataBootstrap(
            CatalogRegistry catalogRegistry,
            MetadataService metadataService,
            TieredMetadataCache metadataCache,
            MetadataEventDispatcher eventDispatcher,
            MetadataSyncService syncService,
            MaterializedViewStore materializedViewStore) {
        this.catalogRegistry = catalogRegistry;
        this.metadataService = metadataService;
        this.metadataCache = metadataCache;
        this.eventDispatcher = eventDispatcher;
        this.syncService = syncService;
        this.materializedViewStore = materializedViewStore;
    }

    /**
     * Initializes the full metadata subsystem from a MetadataStore.
     * Sequence: store → registry → cache → service → initialize catalogs.
     */
    public static MetadataBootstrap initialize(MetadataStore store) {
        return initialize(store, null);
    }

    /**
     * Initializes the metadata subsystem with connector support.
     * When connectorHandleFactory is provided, CONNECTOR-type catalogs can be loaded.
     */
    public static MetadataBootstrap initialize(MetadataStore store, ConnectorHandleFactory connectorHandleFactory) {
        return initialize(store, connectorHandleFactory, null);
    }

    /**
     * Initializes the metadata subsystem with connector and federation support.
     * CONNECTOR-type catalogs use connectorHandleFactory (PluginConnectorHandleFactory).
     * FEDERATION-type catalogs use federationHandleFactory (PluginFederationHandleFactory).
     */
    public static MetadataBootstrap initialize(
            MetadataStore store,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory) {
        TieredMetadataCache cache = new TieredMetadataCache();
        MetadataEventDispatcher dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        CatalogRegistry registry = new CatalogRegistry(
                store, connectorHandleFactory, federationHandleFactory, dispatcher);
        SchemaEvolutionValidator validator = new SchemaEvolutionValidator();

        DefaultMetadataService service = new DefaultMetadataService(
                registry, cache, dispatcher, validator);

        MetadataSyncService syncService = new MetadataSyncService(store, cache, dispatcher);

        MaterializedViewStore materializedViewStore = new InMemoryMaterializedViewStore();

        registry.initialize();
        LOG.info("event=metadata_subsystem_initialized");

        return new MetadataBootstrap(registry, service, cache, dispatcher, syncService, materializedViewStore);
    }

    /**
     * Builds a CalciteSchema bridging MetadataService to Calcite's table resolution.
     * Passes CatalogRegistry so LazyMetadataSchema can resolve tables from connectors.
     */
    public CalciteSchema buildCalciteSchema(org.pangrova.spi.connector.ConnectorSession session) {
        return new MetadataSchemaBuilder(metadataService, catalogRegistry).buildRootSchema(session);
    }

    public CatalogRegistry catalogRegistry() {
        return catalogRegistry;
    }

    public MetadataService metadataService() {
        return metadataService;
    }

    public MetadataSyncService syncService() {
        return syncService;
    }

    public org.pangrova.metadata.cache.TieredMetadataCache metadataCache() {
        return metadataCache;
    }

    public MaterializedViewStore materializedViewStore() {
        return materializedViewStore;
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
