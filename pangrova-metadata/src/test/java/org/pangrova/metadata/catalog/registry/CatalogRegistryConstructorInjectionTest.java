package org.pangrova.metadata.catalog.registry;

import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.event.MetadataEventListener;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.*;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.handle.connector.ConnectorHandleFactory;

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
    void minimalConstructorCreatesRegistryWithoutFactories() {
        registry = new CatalogRegistry(new InMemoryStore());
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
        registry = new CatalogRegistry(store, chf, null, null);
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

        registry = new CatalogRegistry(store, null, null, dispatcher);
        registry.initialize();
        registry.register(internalCatalog("evented"));

        listener.awaitEvents();
        assertThat(listener.events).hasSize(1);
        assertThat(listener.events.get(0)).isInstanceOf(MetadataEvent.CatalogCreatedEvent.class);
    }

    @Test
    void connectorCatalogWithoutFactoryThrows() {
        registry = new CatalogRegistry(new InMemoryStore());
        registry.initialize();

        assertThatThrownBy(() -> registry.register(connectorCatalog("no_factory")))
                .isInstanceOf(Exception.class);
    }

    @Test
    void nullDispatcherDoesNotThrow() {
        registry = new CatalogRegistry(new InMemoryStore(), null, null, null);
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

    static final class InMemoryStore implements MetadataStore {
        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
