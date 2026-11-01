package org.pangrova.coordinator.query.execution.split;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonConnectorFactory;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.ConnectorCatalogHandle;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;

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
        DataSplit bound = DataSplit.parseFrom(NativeReadSplitBinding.bind(
                source.toByteArray(), catalog, session("tenant-a")));
        assertThat(bound.getCatalog()).isEqualTo(CatalogExecutionBindings.from(catalog.getDescriptor()));
        assertThat(bound.toBuilder().clearCatalog().build()).isEqualTo(source);
        assertThat(NativeReadSplitBinding.bind(bound.toByteArray(), catalog, session("tenant-a")))
                .isEqualTo(bound.toByteArray());
    }

    @Test
    void rejectsAStaleOrForgedDeploymentRatherThanOverwritingIt() {
        var stale = CatalogExecutionBindings.from(catalog.getDescriptor()).toBuilder()
                .setCatalogRevision(catalog.getDescriptor().revision() + 1).build();
        assertThatThrownBy(() -> NativeReadSplitBinding.bind(
                split().toBuilder().setCatalog(stale).build().toByteArray(), catalog, session("tenant-a")))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("conflicts");
        assertThatThrownBy(() -> NativeReadSplitBinding.bind(
                split().toBuilder().setConnectorId("other").build().toByteArray(), catalog, session("tenant-a")))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsSuspendedCatalogUnauthorizedTenantAndAnonymousIdentity() {
        assertThatThrownBy(() -> NativeReadSplitBinding.bind(split().toByteArray(), catalog, session("tenant-b")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> NativeReadSplitBinding.bind(split().toByteArray(), catalog, session(null)))
                .isInstanceOf(AccessDeniedException.class);
        catalog.setState(CatalogState.SUSPENDED);
        assertThatThrownBy(() -> NativeReadSplitBinding.bind(split().toByteArray(), catalog, session("tenant-a")))
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
        var actor = new CoordinatorSessionContext("read", Map.of("tenant_id", "forged"), identity, tenant);
        return SessionContextConverter.toConnectorSession(actor, "lake", "default");
    }
}
