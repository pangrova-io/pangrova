package org.pangrova.coordinator.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.materialization.ChangeProof;
import org.pangrova.spi.connector.materialization.ChangeSetMode;
import org.pangrova.spi.connector.materialization.ChangeSetPlan;
import org.pangrova.spi.connector.materialization.MaterializationCapabilities;
import org.pangrova.spi.connector.materialization.PinnedSource;
import org.pangrova.spi.connector.materialization.SourceProvider;
import org.pangrova.spi.connector.materialization.SourceRef;
import org.pangrova.spi.connector.materialization.SourceSnapshotRef;

/** Connector resolution and capability-version fencing for source snapshot holds. */
class CoordinatorSourcePinningTest {

    private static final SourceRef SOURCE = new SourceRef("iceberg", "lake", "db", "orders");
    private static final SourceSnapshotRef SNAPSHOT = new SourceSnapshotRef(
            "iceberg", "snap-1", "a".repeat(64), "b".repeat(64), Instant.EPOCH, true);
    private static final SourceSnapshotRef NEXT_SNAPSHOT = new SourceSnapshotRef(
            "iceberg", "snap-2", "a".repeat(64), "b".repeat(64), Instant.EPOCH, true);

    @Test
    void delegatesPinsToTheOwningConnectorProvider() {
        FakeProvider provider = new FakeProvider(
                MaterializationCapabilities.CURRENT_VERSION);
        CoordinatorSourcePinning pinning = pinning(provider);

        PinnedSource pinned = pinning.pin("tenant", SOURCE, SNAPSHOT, "attempt-1");

        assertThat(pinned.pinOwner()).isEqualTo("attempt-1");
        assertThat(pinned.snapshot()).isEqualTo(SNAPSHOT);
        assertThat(provider.pins).isEqualTo(1);
    }

    @Test
    void rejectsAnUnknownCapabilityVersionBeforePinning() {
        FakeProvider provider = new FakeProvider(
                MaterializationCapabilities.CURRENT_VERSION + 1);
        CoordinatorSourcePinning pinning = pinning(provider);

        assertThatThrownBy(() -> pinning.pin("tenant", SOURCE, SNAPSHOT, "attempt-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported materialization capability version");
        assertThat(provider.pins).isZero();
    }

    @Test
    void rejectsASourceWhoseConnectorDoesNotOwnTheCatalog() {
        FakeProvider provider = new FakeProvider(
                MaterializationCapabilities.CURRENT_VERSION);
        CoordinatorSourcePinning pinning = pinning(provider);
        SourceRef foreign = new SourceRef("paimon", "lake", "db", "orders");

        assertThatThrownBy(() -> pinning.pin("tenant", foreign, SNAPSHOT, "attempt-1"))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("does not match catalog");
    }

    @Test
    void unpinDoesNotRequireCapabilitiesOfADroppedSource() {
        FakeProvider provider = new FakeProvider(
                MaterializationCapabilities.CURRENT_VERSION);
        provider.capabilitiesFailure = new IllegalStateException("source dropped");
        CoordinatorSourcePinning pinning = pinning(provider);

        pinning.unpin(new PinnedSource("tenant", SOURCE, SNAPSHOT, "attempt-1"));

        assertThat(provider.unpins).isEqualTo(1);
    }

    @Test
    void delegatesChangeSetsToTheProviderWhenTheModeIsSupported() {
        FakeProvider provider = pinningProvider();
        CoordinatorSourcePinning pinning = pinning(provider);

        ChangeSetPlan plan = pinning.changeSet(held("snap-1"), held("snap-2"),
                ChangeSetMode.APPEND_ONLY);

        assertThat(provider.changeSets).isEqualTo(1);
        assertThat(plan.fromSnapshot().snapshotToken()).isEqualTo("snap-1");
        assertThat(plan.toSnapshot().snapshotToken()).isEqualTo("snap-2");
        assertThat(pinning.verify(plan).complete()).isTrue();
        assertThat(provider.verifies).isEqualTo(1);
    }

    @Test
    void rejectsChangeSetsTheConnectorCannotProve() {
        FakeProvider provider = new FakeProvider(
                MaterializationCapabilities.CURRENT_VERSION, true, false, true, false, false, false);
        CoordinatorSourcePinning pinning = pinning(provider);

        assertThatThrownBy(() -> pinning.changeSet(held("snap-1"), held("snap-2"),
                ChangeSetMode.APPEND_ONLY))
                .isInstanceOfSatisfying(MaterializationException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(org.pangrova.common.exception.PangrovaErrorCode
                                    .MATERIALIZATION_CAPABILITY_UNAVAILABLE);
                    assertThat(error.getMessage()).contains("APPEND_ONLY");
                });
        assertThat(provider.changeSets).isZero();
    }

    @Test
    void rejectsChangeSetsAcrossDifferentSources() {
        FakeProvider provider = pinningProvider();
        CoordinatorSourcePinning pinning = pinning(provider);
        PinnedSource other = new PinnedSource("tenant",
                new SourceRef("iceberg", "lake", "db", "customers"), NEXT_SNAPSHOT, "attempt-1");

        assertThatThrownBy(() -> pinning.changeSet(held("snap-1"), other,
                ChangeSetMode.APPEND_ONLY))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("same source");
        assertThat(provider.changeSets).isZero();
    }

    private static PinnedSource held(String token) {
        SourceSnapshotRef snapshot = token.equals("snap-2") ? NEXT_SNAPSHOT : SNAPSHOT;
        return new PinnedSource("tenant", SOURCE, snapshot, "attempt-1");
    }

    private static FakeProvider pinningProvider() {
        return new FakeProvider(MaterializationCapabilities.CURRENT_VERSION,
                true, true, true, false, false, false);
    }

    private static CoordinatorSourcePinning pinning(SourceProvider provider) {
        Connector connector = new FakeConnector(provider);
        CatalogRegistry registry = new CatalogRegistry(new EmptyStore(),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.register(new CatalogDescriptor("lake", CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        return new CoordinatorSourcePinning(registry);
    }

    private static final class FakeProvider implements SourceProvider {
        private final int version;
        private final boolean snapshotPin;
        private final boolean appendOnlyProof;
        private final boolean changeSet;
        private final boolean partitionReplace;
        private final boolean affectedKey;
        private final boolean cdc;
        private int pins;
        private int unpins;
        private int changeSets;
        private int verifies;
        private RuntimeException capabilitiesFailure;

        private FakeProvider(int version) {
            this(version, true, false, false, false, false, false);
        }

        private FakeProvider(int version, boolean snapshotPin, boolean appendOnlyProof,
                boolean changeSet, boolean partitionReplace, boolean affectedKey, boolean cdc) {
            this.version = version;
            this.snapshotPin = snapshotPin;
            this.appendOnlyProof = appendOnlyProof;
            this.changeSet = changeSet;
            this.partitionReplace = partitionReplace;
            this.affectedKey = affectedKey;
            this.cdc = cdc;
        }

        @Override
        public String connectorId() {
            return "iceberg";
        }

        @Override
        public MaterializationCapabilities capabilities(SourceRef source) {
            if (capabilitiesFailure != null) {
                throw capabilitiesFailure;
            }
            return new MaterializationCapabilities(version, snapshotPin, appendOnlyProof,
                    changeSet, partitionReplace, affectedKey, cdc, true, true);
        }

        @Override
        public SourceSnapshotRef describe(SourceRef source, String tenant,
                String snapshotToken, String policyFingerprint) {
            return SNAPSHOT;
        }

        @Override
        public PinnedSource pin(SourceRef source, String tenant, SourceSnapshotRef expected,
                String pinOwner) {
            pins++;
            return new PinnedSource(tenant, source, expected, pinOwner);
        }

        @Override
        public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
            changeSets++;
            return new ChangeSetPlan(from.source(), from.snapshot(), to.snapshot(), mode,
                    "change-set-" + changeSets, Instant.EPOCH, java.util.List.of());
        }

        @Override
        public ChangeProof verify(ChangeSetPlan plan) {
            verifies++;
            return new ChangeProof(plan.source(), plan.fromSnapshot().snapshotToken(),
                    plan.toSnapshot().snapshotToken(), plan.mode(), "proof-1", "v1",
                    Instant.EPOCH, true);
        }

        @Override
        public void unpin(PinnedSource pinned) {
            unpins++;
        }
    }

    private static final class FakeConnector implements Connector {
        private final SourceProvider provider;

        private FakeConnector(SourceProvider provider) {
            this.provider = provider;
        }

        @Override
        public Optional<SourceProvider> getMaterializationSourceProvider() {
            return Optional.of(provider);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class EmptyStore implements MetadataStore {
        @Override
        public void saveTable(TableMetadata metadata) {
        }

        @Override
        public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) {
        }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
        }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.empty();
        }

        @Override
        public void deleteCatalogDescriptor(String name) {
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of();
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) {
        }

        @Override
        public void recordSyncHistory(SyncResult result) {
        }

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) {
        }
    }
}
