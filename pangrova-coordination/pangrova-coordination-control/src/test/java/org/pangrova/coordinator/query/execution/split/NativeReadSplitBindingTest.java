package org.pangrova.coordinator.query.execution.split;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonConnectorFactory;
import org.pangrova.coordinator.metadata.CatalogExecutionBindingFactory;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Trusted metadata binding contract; not a Native physical-file execution test. */
class NativeReadSplitBindingTest {

    @TempDir Path warehouse;
    private ConnectorCatalogHandle catalog;

    @BeforeEach
    void openRealCatalog() {
        Map<String, String> properties = Map.of("warehouse", warehouse.toUri().toString());
        var descriptor = new CatalogDescriptor("lake", CatalogType.CONNECTOR, "paimon", CatalogState.ACTIVE,
                properties, "", Set.of("tenant-a"));
        catalog = new ConnectorCatalogHandle(descriptor,
                new PaimonConnectorFactory().create("lake", properties, Optional::empty));
    }

    @AfterEach
    void closeCatalog() {
        catalog.shutdown();
    }

    @Test
    void bindsOnlyTheCurrentTrustedDeploymentAndPreservesPhysicalIdentity() throws Exception {
        DataSplit source = split();
        var binding = NativeReadSplitBinding.prepare(catalog, session("tenant-a"));
        DataSplit bound = binding.bind(source);
        assertThat(bound.getCatalog()).isEqualTo(CatalogExecutionBindingFactory.from(catalog.getDescriptor()));
        assertThat(bound.toBuilder().clearCatalog().build()).isEqualTo(source);
        assertThat(binding.bind(bound)).isSameAs(bound);
        var next = binding.bind(source.toBuilder().setSplitId("next-file").build());
        assertThat(next.getCatalog()).isSameAs(bound.getCatalog());
        catalog.setState(CatalogState.SUSPENDED);
        assertThatThrownBy(() -> binding.bind(source))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed during enumeration");
    }

    @Test
    void bindingRetainsTheConnectorsPhysicalScanEstimate() {
        DataSplit data = split();
        CanonicalConnectorSplit source = new CanonicalConnectorSplit() {
            @Override public DataSplit toDataSplit() { return data; }
            @Override public long estimatedScanBytes() { return 8_192; }
            @Override public long estimatedScanRows() { return 100; }
        };
        var bound = NativeReadSplitBinding.prepare(catalog, session("tenant-a")).bind(source);
        assertThat(bound.estimatedScanRows()).isEqualTo(100);
        assertThat(SplitFetcher.toSplitWithPaths(java.util.List.of(bound))).singleElement().satisfies(stage -> {
            assertThat(stage.estimatedPlacementWeight()).isEqualTo(8_192);
            assertThat(stage.data().toBuilder().clearCatalog().build()).isEqualTo(data);
        });
    }

    @Test
    void rejectsAStaleOrForgedDeploymentRatherThanOverwritingIt() {
        var stale = CatalogExecutionBindingFactory.from(catalog.getDescriptor()).toBuilder()
                .setCatalogRevision(catalog.getDescriptor().revision() + 1).build();
        var binding = NativeReadSplitBinding.prepare(catalog, session("tenant-a"));
        assertThatThrownBy(() -> binding.bind(split().toBuilder().setCatalog(stale).build()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("conflicts");
        assertThatThrownBy(() -> binding.bind(split().toBuilder().setConnectorId("other").build()))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsSuspendedCatalogUnauthorizedTenantAndAnonymousIdentity() {
        assertThatThrownBy(() -> NativeReadSplitBinding.prepare(catalog, session("tenant-b")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> NativeReadSplitBinding.prepare(catalog, session(null)))
                .isInstanceOf(AccessDeniedException.class);
        catalog.setState(CatalogState.SUSPENDED);
        assertThatThrownBy(() -> NativeReadSplitBinding.prepare(catalog, session("tenant-a")))
                .isInstanceOf(AccessDeniedException.class);
    }

    private DataSplit split() {
        return DataSplit.newBuilder().setConnectorId("paimon").setSplitId("pinned-file")
                .setSourceId("lake.default.orders").setSourceSnapshot("7")
                .setDataObjectId("file-1").setOpaqueLocation(warehouse.resolve("data.lance").toUri().toString())
                .setStartOffset(0).setLength(1024).build();
    }

    private static ConnectorSession session(String tenant) {
        var identity = new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
        var actor = new QuerySession("read", Map.of("tenant_id", "forged"), identity, tenant, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        return SessionContextConverter.toConnectorSession(actor, "lake", "default");
    }
}
