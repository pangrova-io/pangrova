package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.metadata.read.MetadataSchemaBuilder;
import org.pangrova.coordinator.metadata.read.PlanningReadBindings;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.spi.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.spi.catalog.FederationHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.metadata.outbox.MetadataSyncService;
import org.apache.calcite.jdbc.CalciteSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
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
    private final MetadataService metadataService;
    private final TieredMetadataCache metadataCache;
    private final MetadataEventDispatcher eventDispatcher;
    private final MetadataSyncService syncService;
    private final org.pangrova.metadata.outbox.TableVersionObserver versionObserver;
    private final DataSource dynamicTablePublicationDataSource;

    private MetadataBootstrap(
            CatalogRegistry catalogRegistry,
            MetadataService metadataService,
            TieredMetadataCache metadataCache,
            MetadataEventDispatcher eventDispatcher,
            MetadataSyncService syncService,
            org.pangrova.metadata.outbox.TableVersionObserver versionObserver,
            DataSource dynamicTablePublicationDataSource) {
        this.catalogRegistry = catalogRegistry;
        this.metadataService = metadataService;
        this.metadataCache = metadataCache;
        this.eventDispatcher = eventDispatcher;
        this.syncService = syncService;
        this.versionObserver = versionObserver;
        this.dynamicTablePublicationDataSource = dynamicTablePublicationDataSource;
    }

    /**
     * Initializes the full metadata subsystem from a CatalogDeploymentRepository.
     * Sequence: store → registry → cache → service → initialize catalogs.
     */
    public static MetadataBootstrap initialize(CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization) {
        return initialize(deployments, tables, projections, synchronization, null);
    }

    /**
     * Initializes the metadata subsystem with connector support.
     * When connectorHandleFactory is provided, CONNECTOR-type catalogs can be loaded.
     */
    public static MetadataBootstrap initialize(CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory) {
        return initialize(deployments, tables, projections, synchronization, connectorHandleFactory, null);
    }

    /**
     * Initializes the metadata subsystem with connector and federation support.
     * CONNECTOR-type catalogs use connectorHandleFactory (PluginConnectorHandleFactory).
     * FEDERATION-type catalogs use federationHandleFactory (PluginFederationHandleFactory).
     */
    public static MetadataBootstrap initialize(
            CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory) {
        return initialize(deployments, tables, projections, synchronization,
                connectorHandleFactory, federationHandleFactory, null);
    }

    /** Initializes durable Dynamic Table publications when the metadata JDBC authority is present. */
    public static MetadataBootstrap initialize(
            CatalogDeploymentRepository deployments,
            TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            ConnectorHandleFactory connectorHandleFactory,
            FederationHandleFactory federationHandleFactory,
            javax.sql.DataSource publicationDataSource) {
        TieredMetadataCache cache = new TieredMetadataCache();
        MetadataEventDispatcher dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        CatalogRegistry registry = new CatalogRegistry(
                deployments, connectorHandleFactory, federationHandleFactory, dispatcher);
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
                versionObserver, publicationDataSource);
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
    public void addMetadataListener(org.pangrova.metadata.spi.event.MetadataEventListener listener) {
        eventDispatcher.addListener(listener);
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

    /** Version observation for LINKED/MANAGED tables, driven by the leader scheduler. */
    public org.pangrova.metadata.outbox.TableVersionObserver versionObserver() {
        return versionObserver;
    }

    public org.pangrova.metadata.cache.TieredMetadataCache metadataCache() {
        return metadataCache;
    }

    /** Returns the durable unified materialization authority when JDBC metadata is available. */
    public org.pangrova.persistence.materialization.postgresql.JdbcMaterializationRepository
            materializationRepository() {
        return dynamicTablePublicationDataSource == null ? null
                : new org.pangrova.persistence.materialization.postgresql.JdbcMaterializationRepository(
                        dynamicTablePublicationDataSource);
    }

    /** Returns the published materialization read model when JDBC metadata is available. */
    public org.pangrova.persistence.materialization.postgresql.JdbcMaterializationCatalog
            materializationCatalog() {
        return dynamicTablePublicationDataSource == null ? null
                : new org.pangrova.persistence.materialization.postgresql.JdbcMaterializationCatalog(
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
