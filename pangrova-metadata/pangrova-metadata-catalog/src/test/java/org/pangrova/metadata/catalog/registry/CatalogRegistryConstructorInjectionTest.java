package org.pangrova.metadata.catalog.registry;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;

import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.event.MetadataEventListener;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;

import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;

/**
 * Validates CatalogRegistry constructor injection replaces setter injection.
 *
 * <p><b>Validates: Requirements 6.3</b></p>
 */
class CatalogRegistryConstructorInjectionTest {

    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (registry != null) registry.shutdown();
        if (dispatcher != null) dispatcher.shutdown();
    }

    @Test
    void maintenanceReadsAndWritesUseTheInjectedTableAuthority() {
        var stores = memoryTableStores();
        registry = new CatalogRegistry(stores, new InMemoryStore());
        var policy = new org.pangrova.table.api.maintenance.TableMaintenancePolicy(
                UUID.randomUUID(), Set.of(), 2, java.time.Duration.ofDays(2),
                java.time.Duration.ofDays(3), true, "admin", java.time.Instant.now());

        registry.tableMaintenance().savePolicy(policy);

        assertThat(stores.maintenance().findPolicy(policy.tableId())).contains(policy);
        assertThat(memoryTableStores().maintenance().findPolicy(policy.tableId())).isEmpty();
    }

    @Test
    void minimalConstructorCreatesRegistryWithoutFactories() {
        registry = new CatalogRegistry(memoryTableStores(), new InMemoryStore());
        registry.initialize();

        // INTERNAL catalogs work without factories
        registry.register(internalCatalog("sys"));
        assertThat(registry.listCatalogNames()).containsExactly("sys");
    }

    @Test
    void connectorFactoryInjectedViaConstructor() {
        InMemoryStore store = new InMemoryStore();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new NoOpConnector());
        registry = new CatalogRegistry(memoryTableStores(), store, chf, null, null);
        registry.initialize();

        registry.register(connectorCatalog("iceberg_cat"));
        assertThat(registry.listCatalogNames()).containsExactly("iceberg_cat");
    }

    @Test
    void eventDispatcherInjectedViaConstructor() throws Exception {
        InMemoryStore store = new InMemoryStore();
        dispatcher = new MetadataEventDispatcher();
        RecordingListener listener = new RecordingListener();
        dispatcher.addListener(listener);
        listener.expectEvents(1);

        registry = new CatalogRegistry(memoryTableStores(), store, null, null, dispatcher);
        registry.initialize();
        registry.register(internalCatalog("evented"));

        listener.awaitEvents();
        assertThat(listener.events).hasSize(1);
        assertThat(listener.events.get(0)).isInstanceOf(MetadataEvent.CatalogCreatedEvent.class);
    }

    @Test
    void connectorCatalogWithoutFactoryThrows() {
        registry = new CatalogRegistry(memoryTableStores(), new InMemoryStore());
        registry.initialize();

        assertThatThrownBy(() -> registry.register(connectorCatalog("no_factory")))
                .isInstanceOf(Exception.class);
    }

    @Test
    void nullDispatcherDoesNotThrow() {
        registry = new CatalogRegistry(memoryTableStores(), new InMemoryStore(), null, null, null);
        registry.initialize();

        assertThatCode(() -> {
            registry.register(internalCatalog("safe"));
            registry.unregister("safe");
        }).doesNotThrowAnyException();
    }

    // ── Helpers ──

    private static CatalogDescriptor internalCatalog(String name) {
        return new CatalogDescriptor(
                name, CatalogType.INTERNAL, "internal",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    private static CatalogDescriptor connectorCatalog(String name) {
        return new CatalogDescriptor(
                name, CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    // ── Test infrastructure ──

    static final class NoOpConnector implements Connector {
        @Override public ConnectorMetadata getMetadata(ConnectorSession s) {
            return new NoOpMetadata();
        }
        @Override public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }
    }

    static final class NoOpMetadata implements ConnectorMetadata {
        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of();
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) { return null; }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) {
            return null;
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) {
            return Map.of();
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }
    }

    static final class RecordingListener implements MetadataEventListener {
        final CopyOnWriteArrayList<MetadataEvent> events = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch = new CountDownLatch(0);

        void expectEvents(int count) { latch = new CountDownLatch(count); }
        void awaitEvents() throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        }

        @Override
        public void onEvent(MetadataEvent event) {
            events.add(event);
            latch.countDown();
        }
    }

    static final class InMemoryStore implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
