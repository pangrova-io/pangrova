package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.MetadataSyncService;
import org.pangrova.metadata.sync.SyncResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MetadataBootstrap initialization sequence with an in-memory MetadataStore.
 * Validates: Requirements 15.1, 15.3, 15.4.
 */
class MetadataBootstrapTest {

    @Test
    void initializeCreatesRegistryAndService() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store);

        assertThat(bootstrap.catalogRegistry()).isNotNull();
        assertThat(bootstrap.metadataService()).isNotNull();

        bootstrap.shutdown();
    }

    @Test
    void initializeLoadsCatalogsFromStore() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.saveCatalogDescriptor(internalCatalog("system"));

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store);

        assertThat(bootstrap.catalogRegistry().listCatalogNames()).contains("system");
        bootstrap.shutdown();
    }

    @Test
    void buildCalciteSchemaReturnsNonNull() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store);

        CalciteSchema schema = bootstrap.buildCalciteSchema(MetadataPlanningTestSessions.session());

        assertThat(schema).isNotNull();
        bootstrap.shutdown();
    }

    @Test
    void shutdownIsIdempotent() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store);

        bootstrap.shutdown();
        // Second shutdown should not throw
        bootstrap.shutdown();
    }

    @Test
    void initializeWiresSyncService() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(store);

        MetadataSyncService syncService = bootstrap.syncService();
        assertThat(syncService).isNotNull();

        bootstrap.shutdown();
    }

    private static CatalogDescriptor internalCatalog(String name) {
        return new CatalogDescriptor(
                name, CatalogType.INTERNAL, "internal",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    /**
     * In-memory MetadataStore for testing without database dependency.
     * Stores catalog descriptors in a simple list.
     */
    static final class InMemoryMetadataStore implements MetadataStore {

        private final java.util.concurrent.CopyOnWriteArrayList<CatalogDescriptor> catalogs =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void saveTable(TableMetadata metadata) { }

        @Override
        public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) { }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            catalogs.removeIf(d -> d.name().equals(descriptor.name()));
            catalogs.add(descriptor);
        }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return catalogs.stream().filter(d -> d.name().equals(name)).findFirst();
        }

        @Override
        public void deleteCatalogDescriptor(String name) {
            catalogs.removeIf(d -> d.name().equals(name));
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.copyOf(catalogs);
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) { return Optional.empty(); }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) { }

        @Override
        public void recordSyncHistory(SyncResult result) { }

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) { }
    }
}
