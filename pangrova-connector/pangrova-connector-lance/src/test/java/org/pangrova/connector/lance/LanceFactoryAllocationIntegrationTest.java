package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.spi.connector.plugin.ConnectorContext;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import java.nio.file.Path;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real durable catalog evidence for allocation; no Native dataset publication is claimed. */
class LanceFactoryAllocationIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void emptyTableAllocationsKeepSqlInsertAndOverwriteSeparateFromPhysicalCreate() throws Exception {
        String name = "allocation_" + UUID.randomUUID();
        var fixture = new LanceManagedSourceFixture(name, warehouse);
        var sdk = new LanceNativeSdkFixture(fixture, warehouse);
        var input = LancePgSdkTestInputs.stream(warehouse, "empty-allocation", 1);
        var allocated = sdk.allocate(input, warehouse.resolve("empty.lance").toString(), "empty-allocation",
                LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, LancePgSdkTestInputs.SCHEMA);
        var table = allocated.output().table();
        var store = fixture.store();
        var connector = new LanceConnectorFactory().create(name, Map.of("warehouse", warehouse.toString()), new ConnectorContext() {
            @Override public Optional<NativeDatasetControlPlane> nativeDatasetControlPlane() { return Optional.empty(); }
            @Override public Optional<ManagedSourceCatalog> managedSourceCatalog() { return Optional.of(fixture.authority()); }
        });
        var session = LanceSplitManagerTest.session("tenant-a");
        var metadata = connector.getMetadata(session);
        var actual = metadata.getTableHandle(session, table.table());
        assertThat(actual).isNotNull();
        assertThat(((LanceTableHandle) actual).tableId()).isEqualTo(table.tableId());
        var insert = (LanceOutputHandle) metadata.beginInsert(session, actual);
        var overwrite = (LanceOutputHandle) metadata.beginOverwrite(session, actual);
        assertThat(insert.allocation().operation()).isEqualTo(NativeWriteAllocation.Operation.CREATE);
        assertThat(insert.writeMode()).isEqualTo("INSERT");
        assertThat(overwrite.allocation().operation()).isEqualTo(NativeWriteAllocation.Operation.CREATE);
        assertThat(overwrite.writeMode()).isEqualTo("OVERWRITE");
        assertThat(overwrite.allocation().writeId()).isNotEqualTo(insert.allocation().writeId());
        assertThat(metadata.getTableMetadata(session, actual).typedColumns().get(0).type()).isEqualTo(PangrovaType.INTEGER);
        assertThatThrownBy(() -> metadata.beginInsert(LanceSplitManagerTest.session("tenant-b"), actual))
                .isInstanceOf(SecurityException.class);
        store.drop(table);
        assertThatThrownBy(() -> metadata.getTableMetadata(session, actual))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incarnation");
    }
}
