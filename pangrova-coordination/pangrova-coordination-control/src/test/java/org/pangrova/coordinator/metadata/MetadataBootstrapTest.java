package org.pangrova.coordinator.metadata;

import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.metadata.outbox.MetadataSyncService;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MetadataBootstrap initialization sequence with an in-memory CatalogDeploymentRepository.
 * Validates: Requirements 15.1, 15.3, 15.4.
 */
class MetadataBootstrapTest {

    @Test
    void initializeCreatesRegistryAndService() {
        InMemoryCatalogDeployments store = new InMemoryCatalogDeployments();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository());

        assertThat(bootstrap.catalogRegistry()).isNotNull();
        assertThat(bootstrap.metadataService()).isNotNull();

        bootstrap.shutdown();
    }

    @Test
    void initializeLoadsCatalogsFromStore() {
        InMemoryCatalogDeployments store = new InMemoryCatalogDeployments();
        store.saveCatalogDescriptor(internalCatalog("system"));

        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository());

        assertThat(bootstrap.catalogRegistry().listCatalogNames()).contains("system");
        bootstrap.shutdown();
    }

    @Test
    void metadataServicesSupplyThePlannerCatalogSchema() {
        InMemoryCatalogDeployments store = new InMemoryCatalogDeployments();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository());

        CalciteSchema schema = new MetadataSchemaBuilder(
                bootstrap.metadataService(), bootstrap.catalogRegistry())
                .buildRootSchema(MetadataPlanningTestSessions.session());

        assertThat(schema).isNotNull();
        bootstrap.shutdown();
    }

    @Test
    void shutdownIsIdempotent() {
        InMemoryCatalogDeployments store = new InMemoryCatalogDeployments();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository());

        bootstrap.shutdown();
        // Second shutdown should not throw
        bootstrap.shutdown();
    }

    @Test
    void initializeWiresSyncService() {
        InMemoryCatalogDeployments store = new InMemoryCatalogDeployments();
        MetadataBootstrap bootstrap = MetadataBootstrap.initialize(memoryTableStores(), store, new org.pangrova.coordinator.metadata.UnsupportedTableProjectionRepository(), new org.pangrova.coordinator.metadata.UnsupportedMetadataProjectionTransactions(), new org.pangrova.coordinator.metadata.UnsupportedMetadataSyncRepository());

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
     * In-memory CatalogDeploymentRepository for testing without database dependency.
     * Stores catalog descriptors in a simple list.
     */
    static final class InMemoryCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final java.util.concurrent.CopyOnWriteArrayList<CatalogDescriptor> catalogs =
                new java.util.concurrent.CopyOnWriteArrayList<>();

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
        public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) {
            return catalogs.removeIf(d -> d.identity().equals(expected));
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.copyOf(catalogs);
        }

    }
}
