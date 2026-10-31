package org.pangrova.metadata.core.catalog.registry;

import org.pangrova.metadata.core.event.MetadataEvent;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.core.event.MetadataEventListener;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;

/**
 * Validates CatalogRegistry dispatches MetadataEvents on register/unregister.
 * Validates: Requirements 76.1, 76.2.
 */
class CatalogRegistryEventDispatchTest {

    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;
    private RecordingListener listener;

    @BeforeEach
    void setUp() {
        InMemoryStore store = new InMemoryStore();
        dispatcher = new MetadataEventDispatcher();
        listener = new RecordingListener();
        dispatcher.addListener(listener);
        registry = new CatalogRegistry(store, null, null, dispatcher);
        registry.initialize();
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void registerCatalogDispatchesCatalogCreatedEvent() throws Exception {
        listener.expectEvents(1);
        CatalogDescriptor descriptor = internalCatalog("test_catalog");

        registry.register(descriptor);

        listener.awaitEvents();
        assertThat(listener.events).hasSize(1);
        assertThat(listener.events.get(0)).isInstanceOf(MetadataEvent.CatalogCreatedEvent.class);
        MetadataEvent.CatalogCreatedEvent event = (MetadataEvent.CatalogCreatedEvent) listener.events.get(0);
        assertThat(event.descriptor().name()).isEqualTo("test_catalog");
    }

    @Test
    void unregisterCatalogDispatchesCatalogDroppedEvent() throws Exception {
        listener.expectEvents(1);
        registry.register(internalCatalog("to_drop"));
        listener.awaitEvents();
        listener.clear();

        listener.expectEvents(1);
        registry.unregister("to_drop");

        listener.awaitEvents();
        assertThat(listener.events).hasSize(1);
        assertThat(listener.events.get(0)).isInstanceOf(MetadataEvent.CatalogDroppedEvent.class);
        MetadataEvent.CatalogDroppedEvent event = (MetadataEvent.CatalogDroppedEvent) listener.events.get(0);
        assertThat(event.catalogName()).isEqualTo("to_drop");
    }

    @Test
    void noDispatcherDoesNotThrow() {
        InMemoryStore store = new InMemoryStore();
        CatalogRegistry plain = new CatalogRegistry(store);
        plain.initialize();

        // No dispatcher set — register should not throw
        plain.register(internalCatalog("safe"));
        plain.unregister("safe");
        plain.shutdown();
    }

    @Test
    void registrationFailureAttachesCleanupFailureAsSuppressed() {
        // F-META-002: when catalog registration fails (loadCatalog cannot resolve the connector) AND
        // compensation cleanup also fails (deleteCatalogDescriptor throws), the re-thrown
        // registration error must carry the cleanup failure as a SUPPRESSED exception — so neither
        // failure is silently swallowed.
        InMemoryStore failingStore = new InMemoryStore();
        failingStore.failDeleteCatalogDescriptor = true;
        CatalogRegistry reg = new CatalogRegistry(failingStore, null, null, dispatcher);
        reg.initialize();
        try {
            // CONNECTOR-type catalog with a null connector factory → loadCatalog throws.
            CatalogDescriptor connectorCatalog = new CatalogDescriptor(
                    "bad_connector", CatalogType.CONNECTOR, "nonexistent",
                    CatalogState.ACTIVE, Map.of(), "", Set.of());

            Throwable thrown = catchThrowable(() -> reg.register(connectorCatalog));

            assertThat(thrown)
                    .as("registration fails: no connector factory to load the CONNECTOR catalog")
                    .isNotNull();
            assertThat(thrown.getSuppressed())
                    .as("F-META-002: cleanup failure surfaced as suppressed, not swallowed")
                    .anySatisfy(s -> assertThat(s.getMessage())
                            .contains("simulated cleanup delete failure"));
        } finally {
            reg.shutdown();
        }
    }

    private static CatalogDescriptor internalCatalog(String name) {
        return new CatalogDescriptor(
                name, CatalogType.INTERNAL, "internal",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    /**
     * Listener that records events with a latch for async dispatch synchronization.
     */
    static final class RecordingListener implements MetadataEventListener {
        final CopyOnWriteArrayList<MetadataEvent> events = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch = new CountDownLatch(0);

        void expectEvents(int count) {
            latch = new CountDownLatch(count);
        }

        void awaitEvents() throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS))
                    .as("Timed out waiting for events")
                    .isTrue();
        }

        void clear() {
            events.clear();
        }

        @Override
        public void onEvent(MetadataEvent event) {
            events.add(event);
            latch.countDown();
        }
    }

    /**
     * Minimal in-memory MetadataStore for unit testing.
     */
    static final class InMemoryStore implements MetadataStore {
        private final java.util.concurrent.CopyOnWriteArrayList<CatalogDescriptor> catalogs =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override public void saveTable(TableMetadata metadata) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return catalogs.stream().filter(d -> d.name().equals(name)).findFirst();
        }
        volatile boolean failDeleteCatalogDescriptor = false;
        @Override public void deleteCatalogDescriptor(String name) {
            if (failDeleteCatalogDescriptor) {
                throw new RuntimeException("simulated cleanup delete failure");
            }
            catalogs.removeIf(d -> d.name().equals(name));
        }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
