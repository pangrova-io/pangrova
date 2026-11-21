package org.pangrova.metadata.outbox;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.cache.memory.MemoryCacheStoreFactory;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogHealthMonitor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.catalog.federation.FederationCatalogHandle;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.catalog.CatalogMetadata;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.jdbc.support.PooledDataSourceFactory;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.postgresql.ds.PGSimpleDataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Live collaborator identity only. The real PG sourceDeployments is a component fixture, not a production provider. */
class MetadataBorrowedOwnershipIntegrationTest {
    private PostgresCatalogDeploymentRepository deployments;
    private PostgresTableProjectionRepository tables;
    private PostgresMetadataProjectionTransactions projections;
    private CurrentCatalogProjectionFixture projectionFixture;
    private PostgresFederationInventory source;
    private TieredMetadataCache cache;
    private MetadataEventDispatcher dispatcher;
    private CatalogRegistry registry;
    private CatalogMetadata service;
    private MetadataSyncService sync;
    private PostgresCatalogDeploymentRepository sourceDeployments;
    private PostgresMetadataProjectionTransactions sourceProjections;
    private CurrentCatalogProjectionFixture sourceFixture;

    @BeforeEach
    void setUp() throws SQLException {
        var inventorySource = SharedPostgresContainer.migratedDataSource("borrow_inventory");
        sourceDeployments = new PostgresCatalogDeploymentRepository(inventorySource);
        sourceProjections = new PostgresMetadataProjectionTransactions(inventorySource);
        sourceFixture = new CurrentCatalogProjectionFixture(sourceDeployments, sourceProjections);
        sourceDeployments.saveCatalogDescriptor(descriptor("cat", CatalogType.FEDERATION));
        sourceFixture.saveSchemas("cat", List.of("sch"));
        source = new PostgresFederationInventory(inventorySource, "cat");
        var directory = SharedPostgresContainer.migratedDataSource("borrow_directory");
        deployments = new PostgresCatalogDeploymentRepository(directory);
        tables = new PostgresTableProjectionRepository(directory);
        projections = new PostgresMetadataProjectionTransactions(directory);
        projectionFixture = new CurrentCatalogProjectionFixture(deployments, projections);
        deployments.saveCatalogDescriptor(descriptor("cat", CatalogType.FEDERATION));
        projectionFixture.saveSchemas("cat", List.of("sch"));
        cache = new TieredMetadataCache();
        dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        registry = new CatalogRegistry(memoryTableStores(), deployments, null,
                descriptor -> new FederationCatalogHandle(descriptor, source), dispatcher);
        registry.initialize();
        service = new CatalogMetadata(registry, cache, dispatcher, new SchemaEvolutionValidator());
        sync = new MetadataSyncService(tables, projections, new PostgresMetadataSyncRepository(directory), cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        if (registry != null) registry.shutdown();
        if (dispatcher != null) dispatcher.shutdown();
        if (source != null) source.close();
    }

    @Test
    void jdbcStoreBorrowsTheExternallyOwnedProductionPool() throws SQLException {
        var pg = (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("borrow_pool");
        var pool = PooledDataSourceFactory.create(pg.getURL(), pg.getUser(), pg.getPassword());
        try {
            var borrowed = new PostgresCatalogDeploymentRepository(pool);
            var descriptor = descriptor("pooled", CatalogType.INTERNAL);
            borrowed.saveCatalogDescriptor(descriptor);
            assertThat(borrowed.getCatalogDescriptor("pooled")).contains(descriptor);
            assertThat(pool.isClosed()).isFalse();
            pool.close();
            assertThatThrownBy(() -> borrowed.getCatalogDescriptor("pooled"))
                    .hasRootCauseInstanceOf(SQLException.class)
                    .hasRootCauseMessage("HikariDataSource HikariDataSource (pangrova-metadata-pool) has been closed.");
        } finally {
            pool.close();
        }
    }

    @Test
    void registryAndDefaultServiceReadTheSameDurableDirectoryAndCache() {
        var session = session();
        assertThat(service.listSchemas(session, "cat")).containsExactly("sch");
        var peer = descriptor("peer", CatalogType.INTERNAL);
        deployments.saveCatalogDescriptor(peer);
        assertThat(service.getCatalogDescriptor("peer")).contains(peer);
        assertThat(service.getCatalogDescriptor("missing")).isEmpty();
        registry.reconcileFromStore();

        assertThat(service.listCatalogs(session)).contains("cat", "peer");
        assertThat(service.getCatalogDescriptor("peer")).contains(peer);
        var metadata = table("cat", "cached", "original");
        cache.putTable("cat", "sch", "cached", metadata);
        service.invalidateTableCaches(new QualifiedTableRef("cat", "sch", "cached"));
        assertThat(cache.getTable("cat", "sch", "cached")).isEmpty();
        service.createCatalog(session, descriptor("created", CatalogType.INTERNAL));
        assertThat(deployments.getCatalogDescriptor("created")).isPresent();
    }

    @Test
    void registryDispatchesToTheOriginalLiveCacheNotADetachedBus() {
        var peer = descriptor("peer", CatalogType.INTERNAL);
        registry.register(peer);
        projectionFixture.saveSchemas("peer", List.of("sch"));
        var metadata = table("peer", "cached", "original");
        projectionFixture.saveTable(metadata);
        cache.putTable("peer", "sch", "cached", metadata);

        registry.unregister("peer");

        assertThat(deployments.getCatalogDescriptor("peer")).isEmpty();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cache.getTable("peer", "sch", "cached")).isEmpty());
    }

    @Test
    void syncUsesTheOriginalStoreCacheAndDispatcher() {
        var untouched = table("cat", "untouched", "local");
        projectionFixture.saveTable(untouched);
        cache.putTable("cat", "sch", "untouched", untouched);
        sourceFixture.saveTable(table("cat", "changed", "remote"));
        var actual = source.getTable("sch", "changed");

        var result = sync.incrementalSync(deployments.getCatalogDescriptor("cat").orElseThrow(), source);

        assertThat(result.errors()).isZero();
        assertThat(tables.loadTable("cat", "sch", "changed")).contains(actual);
        // The sync directly invalidates only 'changed'; the actual bus clears the untouched sibling.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(cache.getTable("cat", "sch", "untouched")).isEmpty());
    }

    @Test
    void tieredCacheUsesTheSameActualL2Adapter() {
        var l2 = new MemoryCacheStoreFactory().open(java.util.Map.of());
        var writer = new TieredMetadataCache(100, 60, 10, l2);
        var reader = new TieredMetadataCache(100, 60, 10, l2);
        var metadata = table("cat", "shared", "actual");

        writer.putTable("cat", "sch", "shared", metadata);

        assertThat(reader.getTable("cat", "sch", "shared")).contains(metadata);
        writer.invalidateTable("cat", "sch", "shared");
        var freshReader = new TieredMetadataCache(100, 60, 10, l2);
        assertThat(freshReader.getTable("cat", "sch", "shared")).isEmpty();
    }

    @Test
    void healthMonitorBorrowsTheLiveMapRatherThanACatalogSnapshot() {
        var live = new ConcurrentHashMap<String, CatalogHandle>();
        var monitor = new CatalogHealthMonitor(live);
        var handle = new FederationCatalogHandle(descriptor("cat", CatalogType.FEDERATION), source);
        handle.setState(CatalogState.DEGRADED);
        // Added after monitor construction: a defensive map copy would never see this real handle.
        live.put("cat", handle);
        try {
            monitor.start();
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                    assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE));
        } finally {
            monitor.stop();
        }
        // This proves borrowed map identity only, not healthy production connectors or stop/join semantics.
    }

    private static MetadataSessionContext session() {
        return new MetadataSessionContext("reader", "cat", "sch", "UTC", "en", Map.of(),
                "tenant", "", "component-query");
    }

    private static CatalogDescriptor descriptor(String name, CatalogType type) {
        return new CatalogDescriptor(name, type, type == CatalogType.INTERNAL ? "internal" : "sourceDeployments",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    private static TableMetadata table(String catalog, String name, String marker) {
        return new TableMetadata(catalog, "sch", name, List.of(), Map.of(), List.of(), Map.of("marker", marker),
                Instant.EPOCH, Instant.EPOCH);
    }
}
