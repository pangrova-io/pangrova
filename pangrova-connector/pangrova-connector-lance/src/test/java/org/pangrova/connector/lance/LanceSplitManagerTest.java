package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.spi.connector.metadata.NativeDatasetScanPage;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceSplitManagerTest extends org.pangrova.testing.ContainerTestBase {
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path warehouse;

    @Test
    void partitionSplitPinsManifestVersionAndRejectsByteRangeSubstitution() throws Exception {
        var fixture = new LanceManagedSourceFixture("splits_" + java.util.UUID.randomUUID(), warehouse);
        var sdk = new LanceNativeSdkFixture(fixture, warehouse);
        String uri = warehouse.resolve("pinned.lance").toString();
        var input = LancePgSdkTestInputs.stream(warehouse, "pinned-create", 1, 2, 3, 4, 5, 6, 7, 8, 9);
        var allocation = sdk.allocate(input, uri, "pinned-create", org.pangrova.protocol.v2.LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE,
                0, 0, LancePgSdkTestInputs.SCHEMA);
        var empty = allocation.output().table();
        var fragments = new java.util.ArrayList<byte[]>();
        for (int partition = 0; partition <= 42; partition++) {
            var actual = sdk.allocate(input, uri, "pinned-create", org.pangrova.protocol.v2.LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE,
                    0, partition, LancePgSdkTestInputs.SCHEMA);
            fragments.add(sdk.write(input, actual));
        }
        var accepted = sdk.accept(allocation, sdk.prepare(allocation, fragments));
        for (long base = 1; base < 7; base++) {
            var appendInput = LancePgSdkTestInputs.stream(warehouse, "pinned-append-" + base, (int) base);
            var append = sdk.allocate(appendInput, uri, "pinned-append-" + base,
                    org.pangrova.protocol.v2.LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, base, 0, LancePgSdkTestInputs.SCHEMA);
            accepted = sdk.accept(append, sdk.prepare(append, java.util.List.of(sdk.write(appendInput, append))));
        }
        var pinned = fixture.store().load(empty.catalog(), empty.tenantId(), empty.table());
        var page = sdk.scanPage(accepted, java.util.OptionalLong.empty(), 64);
        var actualPartition = page.getPartitionsList().stream().filter(partition -> partition.getPartitionId() == 42)
                .findFirst().orElseThrow();
        assertThat(actualPartition.getPhysicalRows()).isEqualTo(9);
        var catalog = fixture.catalog();
        var split = new LanceDataSplit(pinned, new NativeDatasetScanPage.Partition(actualPartition.getPartitionId(), actualPartition.getPhysicalRows()),
                catalog.toByteArray(), accepted.getManifestSha256().getDigest().toByteArray(),
                org.pangrova.protocol.SourceSnapshotBindings.decode(accepted, catalog));
        var proto = DataSplit.parseFrom(split.toSplitInfoProto());
        assertThat(proto.getSourceSnapshot()).isEqualTo("7");
        assertThat(proto.getOpaqueLocation()).isEqualTo(pinned.datasetUri());
        assertThat(DataSplitProtoCodec.fileDetail(proto).getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_LANCE);
        assertThat(DataSplitProtoCodec.fileDetail(proto).getScanPartition().getPartitionId()).isEqualTo(42);
        assertThat(DataSplitProtoCodec.fileDetail(proto).getScanPartition().getAcceptedSnapshot()).isEqualTo(accepted);
        assertThat(proto.getCatalog()).isEqualTo(catalog);
        assertThat(proto.getStartOffset()).isZero(); assertThat(proto.getLength()).isZero();
        assertThat(new LancePartitionSplitSource(empty, session("tenant-a"), Optional.empty(), null).isFinished()).isTrue();
        var manager = new LanceSplitManager(Optional.empty(), () -> fixture.store(), empty.catalog(), warehouse.toString());
        assertThatThrownBy(() -> manager.getSplits(ConnectorTransactionHandle.EMPTY, session("tenant-b"), pinned,
                DynamicFilter.ALL, Constraint.EMPTY)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> manager.getSplits(ConnectorTransactionHandle.EMPTY, session("tenant-a"), pinned,
                DynamicFilter.ALL, Constraint.EMPTY)).isInstanceOf(IllegalStateException.class).hasMessageContaining("Native control");
    }

    static ConnectorSession session(String tenant) {
        return new ConnectorSession() {
            @Override
            public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
                return java.util.Optional.empty();
            }

            @Override public String getUser() { return "test-user"; }
            @Override public Optional<String> getCatalog() { return Optional.of("lance_catalog"); }
            @Override public Optional<String> getSchema() { return Optional.of("db"); }
            @Override public Map<String, String> getProperties() { return Map.of("tenant_id", tenant); }
        };
    }
}
