package org.pangrova.metadata.catalog.registry;

import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;

/**
 * Tests for G-META-02 / Task 38: peer divergence detection and repair during
 * leader election via {@link CatalogRegistry#reconcileFromStore()}.
 */
class CatalogRegistryReconcileFromStoreTest {

    private CatalogRegistry registry;

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.shutdown();
        }
    }

    @Test
    void reconcileLoadsMissingCatalogsFromStore() {
        InMemoryStore store = new InMemoryStore();
        store.saveCatalogDescriptor(internalCatalog("existing"));

        registry = new CatalogRegistry(store);
        registry.initialize();
        assertThat(registry.listCatalogNames()).containsExactly("existing");

        // Simulate a peer adding a catalog while this node was standby
        store.saveCatalogDescriptor(internalCatalog("peer_added"));

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.loaded()).containsExactly("peer_added");
        assertThat(report.removed()).isEmpty();
        assertThat(report.loadFailed()).isEmpty();
        assertThat(report.isEmpty()).isFalse();
        assertThat(registry.listCatalogNames())
                .containsExactlyInAnyOrder("existing", "peer_added");
    }

    @Test
    void reconcileRemovesOrphanedCatalogsNotInStore() {
        InMemoryStore store = new InMemoryStore();
        store.saveCatalogDescriptor(internalCatalog("keep"));
        store.saveCatalogDescriptor(internalCatalog("will_drop"));

        registry = new CatalogRegistry(store);
        registry.initialize();
        assertThat(registry.listCatalogNames())
                .containsExactlyInAnyOrder("keep", "will_drop");

        // Simulate a peer dropping "will_drop" from the shared store
        store.deleteCatalogDescriptor("will_drop");

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.loaded()).isEmpty();
        assertThat(report.removed()).containsExactly("will_drop");
        assertThat(report.loadFailed()).isEmpty();
        assertThat(registry.listCatalogNames()).containsExactly("keep");
    }

    @Test
    void reconcileHandlesBothMissingAndOrphanedSimultaneously() {
        InMemoryStore store = new InMemoryStore();
        store.saveCatalogDescriptor(internalCatalog("shared"));
        store.saveCatalogDescriptor(internalCatalog("local_only"));

        registry = new CatalogRegistry(store);
        registry.initialize();

        // Peer dropped "local_only" and added "remote_new"
        store.deleteCatalogDescriptor("local_only");
        store.saveCatalogDescriptor(internalCatalog("remote_new"));

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.loaded()).containsExactly("remote_new");
        assertThat(report.removed()).containsExactly("local_only");
        assertThat(registry.listCatalogNames())
                .containsExactlyInAnyOrder("shared", "remote_new");
    }

    @Test
    void reconcileReportsNoDivergenceWhenAligned() {
        InMemoryStore store = new InMemoryStore();
        store.saveCatalogDescriptor(internalCatalog("alpha"));
        store.saveCatalogDescriptor(internalCatalog("beta"));

        registry = new CatalogRegistry(store);
        registry.initialize();

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.isEmpty()).isTrue();
        assertThat(report.loaded()).isEmpty();
        assertThat(report.removed()).isEmpty();
        assertThat(report.loadFailed()).isEmpty();
    }

    @Test
    void reconcileIsolatesLoadFailuresWithoutAffectingOtherCatalogs() {
        InMemoryStore store = new InMemoryStore();
        store.saveCatalogDescriptor(internalCatalog("existing"));

        registry = new CatalogRegistry(store, null, null, null);
        registry.initialize();

        // Add a CONNECTOR catalog (no factory, will fail) and an INTERNAL one
        store.saveCatalogDescriptor(connectorCatalog("will_fail"));
        store.saveCatalogDescriptor(internalCatalog("will_succeed"));

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.loaded()).contains("will_succeed");
        assertThat(report.loadFailed()).contains("will_fail");
        assertThat(registry.listCatalogNames())
                .containsExactlyInAnyOrder("existing", "will_succeed");
    }

    @Test
    void reconcileOnEmptyRegistryLoadsAllFromStore() {
        InMemoryStore store = new InMemoryStore();

        registry = new CatalogRegistry(store);
        registry.initialize();
        assertThat(registry.listCatalogNames()).isEmpty();

        store.saveCatalogDescriptor(internalCatalog("first"));
        store.saveCatalogDescriptor(internalCatalog("second"));

        CatalogRegistry.DivergenceReport report = registry.reconcileFromStore();

        assertThat(report.loaded()).containsExactlyInAnyOrder("first", "second");
        assertThat(report.removed()).isEmpty();
        assertThat(registry.listCatalogNames())
                .containsExactlyInAnyOrder("first", "second");
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

    // ── In-memory store ──

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
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
