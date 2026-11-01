package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.metadata.read.MetadataSchemaBuilder;
import org.pangrova.coordinator.metadata.read.PlanningReadBindings;

import org.pangrova.metadata.api.MetadataGateway;
import org.pangrova.metadata.core.cache.TieredMetadataCache;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.core.control.JdbcTableControlGovernanceBindingStore;
import org.pangrova.metadata.core.control.JdbcTableControlStatisticsStore;
import org.pangrova.metadata.core.control.JdbcTableControlStore;
import org.pangrova.metadata.core.control.TableControlService;
import org.pangrova.metadata.core.control.TableControlSettings;
import org.pangrova.metadata.core.control.TableControlStatisticsService;
import org.pangrova.metadata.core.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.core.catalog.FederationHandleFactory;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.core.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.core.DefaultMetadataService;
import org.pangrova.metadata.core.store.MetadataStore;
import org.pangrova.metadata.core.sync.MetadataSyncService;
import org.apache.calcite.jdbc.CalciteSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Encapsulates the metadata subsystem initialization sequence for Coordinator startup.
 * Creates CatalogRegistry, MetadataService, TieredMetadataCache, and Calcite schema bridge.
 */
public final class MetadataBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataBootstrap.class);

    private final CatalogRegistry catalogRegistry;
    private final DefaultMetadataService defaultMetadataService;
    private final MetadataGateway metadataGateway;
    private final TieredMetadataCache metadataCache;
    private final MetadataEventDispatcher eventDispatcher;
    private final MetadataSyncService syncService;
    private final org.pangrova.metadata.core.sync.TableVersionObserver versionObserver;
    private final DataSource dynamicTablePublicationDataSource;

    private MetadataBootstrap(
            CatalogRegistry catalogRegistry,
            DefaultMetadataService defaultMetadataService,
            MetadataGateway metadataGateway,
            TieredMetadataCache metadataCache,
            MetadataEventDispatcher eventDispatcher,
            MetadataSyncService syncService,
            org.pangrova.metadata.core.sync.TableVersionObserver versionObserver,
            DataSource dynamicTablePublicationDataSource) {
        this.catalogRegistry = catalogRegistry;
        this.defaultMetadataService = defaultMetadataService;
        this.metadataGateway = metadataGateway;
        this.metadataCache = metadataCache;
        this.eventDispatcher = eventDispatcher;
        this.syncService = syncService;
        this.versionObserver = versionObserver;
        this.dynamicTablePublicationDataSource = dynamicTablePublicationDataSource;
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
        return initialize(store, connectorHandleFactory, federationHandleFactory, null);
    }

    /**
     * Initializes the metadata subsystem for tests and explicit metadata-less modes.
     *
     * <p>Production embedders must use {@link #initializeDurable} so table control,
     * statistics and governance bindings are backed by the shared JDBC authority before
     * any table is resolved.</p>
     */
    public static MetadataBootstrap initialize(
            MetadataStore store,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            javax.sql.DataSource publicationDataSource) {
        return build(store, connectorHandleFactory, federationHandleFactory, publicationDataSource, null);
    }

    /**
     * Initializes the metadata subsystem with durable table control, statistics and
     * governance bindings. This is the production entry point; it never falls back to
     * the in-memory control authority.
     */
    public static MetadataBootstrap initializeDurable(
            MetadataStore store,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            javax.sql.DataSource publicationDataSource,
            javax.sql.DataSource controlDataSource) {
        return build(store, connectorHandleFactory, federationHandleFactory,
                publicationDataSource, controlDataSource);
    }

    private static MetadataBootstrap build(
            MetadataStore store,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            javax.sql.DataSource publicationDataSource,
            javax.sql.DataSource controlDataSource) {
        TieredMetadataCache cache = new TieredMetadataCache();
        MetadataEventDispatcher dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        CatalogRegistry registry = new CatalogRegistry(
                store, connectorHandleFactory, federationHandleFactory, dispatcher);
        if (controlDataSource != null) {
            TableControlService control = new TableControlService(
                    new JdbcTableControlStore(controlDataSource),
                    registry::requireCurrentCatalogIdentity,
                    TableControlSettings.fromSystemProperties());
            registry.setTableControlService(control);
            registry.setTableStatisticsService(new TableControlStatisticsService(
                    control, new JdbcTableControlStatisticsStore(controlDataSource)));
            registry.setGovernanceBindingStore(
                    new JdbcTableControlGovernanceBindingStore(controlDataSource));
        }
        SchemaEvolutionValidator validator = new SchemaEvolutionValidator();

        DefaultMetadataService service = new DefaultMetadataService(
                registry, cache, dispatcher, validator);

        MetadataSyncService syncService = new MetadataSyncService(store, cache, dispatcher);
        org.pangrova.metadata.core.sync.TableVersionObserver versionObserver =
                new org.pangrova.metadata.core.sync.TableVersionObserver(registry, dispatcher);

        registry.initialize();
        LOG.info("event=metadata_subsystem_initialized tableControl={}",
                controlDataSource == null ? "in-memory" : "jdbc");
        MetadataGateway gateway = new MetadataGateway(
                service, service, service, service, service);
        return new MetadataBootstrap(registry, service, gateway, cache, dispatcher, syncService,
                versionObserver, publicationDataSource);
    }

    /**
     * Builds a CalciteSchema bridging MetadataService to Calcite's table resolution.
     * Passes CatalogRegistry so LazyMetadataSchema can resolve tables from connectors.
     */
    public CalciteSchema buildCalciteSchema(org.pangrova.spi.connector.ConnectorSession session) {
        return new MetadataSchemaBuilder(metadataGateway, catalogRegistry).buildRootSchema(session);
    }

    /** Shares the selected source owner with the session planner's statistics provider. */
    public CalciteSchema buildCalciteSchema(org.pangrova.spi.connector.ConnectorSession session,
            PlanningReadBindings readBindings) {
        return new MetadataSchemaBuilder(metadataGateway, catalogRegistry).buildRootSchema(session, readBindings);
    }

    /** Subscribes to metadata lifecycle events such as observed external commits. */
    public void addMetadataListener(org.pangrova.metadata.core.event.MetadataEventListener listener) {
        eventDispatcher.addListener(listener);
    }

    public CatalogRegistry catalogRegistry() {
        return catalogRegistry;
    }

    /** Implementation used only by the assembly for tag-service wiring. */
    public DefaultMetadataService defaultMetadataService() {
        return defaultMetadataService;
    }

    /** Use-case ports; prefer a single port over the aggregate. */
    public MetadataGateway metadataGateway() {
        return metadataGateway;
    }

    public MetadataSyncService syncService() {
        return syncService;
    }

    /** Version observation for LINKED/MANAGED tables, driven by the leader scheduler. */
    public org.pangrova.metadata.core.sync.TableVersionObserver versionObserver() {
        return versionObserver;
    }

    public org.pangrova.metadata.core.cache.TieredMetadataCache metadataCache() {
        return metadataCache;
    }

    /** Returns the durable unified materialization authority when JDBC metadata is available. */
    public org.pangrova.materialization.store.jdbc.JdbcMaterializationRepository
            materializationRepository() {
        return dynamicTablePublicationDataSource == null ? null
                : new org.pangrova.materialization.store.jdbc.JdbcMaterializationRepository(
                        dynamicTablePublicationDataSource);
    }

    /** Returns the published materialization read model when JDBC metadata is available. */
    public org.pangrova.materialization.store.jdbc.JdbcMaterializationCatalog
            materializationCatalog() {
        return dynamicTablePublicationDataSource == null ? null
                : new org.pangrova.materialization.store.jdbc.JdbcMaterializationCatalog(
                        dynamicTablePublicationDataSource);
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
