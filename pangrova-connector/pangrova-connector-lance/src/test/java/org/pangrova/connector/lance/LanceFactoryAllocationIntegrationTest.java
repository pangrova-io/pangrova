package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.testing.ContainerTestBase;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real durable catalog evidence for allocation; no Native dataset publication is claimed. */
class LanceFactoryAllocationIntegrationTest extends ContainerTestBase {
    @Test
    void emptyTableAllocationsKeepSqlInsertAndOverwriteSeparateFromPhysicalCreate() {
        var postgres = LanceCatalogTestDatabase.instance();
        var store = new LanceCatalogStore(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        store.createSchema("lance_catalog", "tenant-a", "db");
        var table = LanceOutputHandleCodecTest.table("tenant-a", "empty_" + UUID.randomUUID());
        store.create(table);
        var connector = new LanceConnectorFactory().create("lance_catalog", Map.of(
                "jdbc.url", postgres.getJdbcUrl(), "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(), "warehouse", "s3://warehouse"), Optional::empty);
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
