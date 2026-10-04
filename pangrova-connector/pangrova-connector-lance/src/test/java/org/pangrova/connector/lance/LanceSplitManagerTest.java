package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.spi.connector.metadata.NativeDatasetScanPage;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceSplitManagerTest {
    @Test
    void partitionSplitPinsManifestVersionAndRejectsByteRangeSubstitution() throws Exception {
        var empty = LanceOutputHandleCodecTest.table("tenant-a", "pinned");
        var pinned = new LanceTableHandle(empty.catalog(), empty.tenantId(), empty.table(), empty.tableId(), empty.datasetUri(),
                empty.columns(), empty.properties(), empty.schemaFence(), 7, 2, 9);
        var catalog = CatalogExecutionBinding.newBuilder().setCatalogName(empty.catalog())
                .setConnectorProvider("lance").setCatalogIncarnationId("incarnation").setCatalogRevision(1)
                .setConfigurationFingerprint(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(new byte[32]))).build();
        var split = new LanceDataSplit(pinned, new NativeDatasetScanPage.Partition(42, 9),
                catalog.toByteArray(), new byte[32]);
        var proto = DataSplit.parseFrom(split.toSplitInfoProto());
        assertThat(proto.getSourceSnapshot()).isEqualTo("7");
        assertThat(proto.getOpaqueLocation()).isEqualTo(pinned.datasetUri());
        assertThat(DataSplitProtoCodec.fileDetail(proto).getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_LANCE);
        assertThat(DataSplitProtoCodec.fileDetail(proto).getScanPartition().getPartitionId()).isEqualTo(42);
        assertThat(proto.getCatalog()).isEqualTo(catalog);
        assertThat(proto.getStartOffset()).isZero();
        assertThat(proto.getLength()).isZero();
        assertThat(new LancePartitionSplitSource(empty, session("tenant-a"), Optional.empty()).isFinished()).isTrue();
        var manager = new LanceSplitManager(Optional.empty(), () -> {
            throw new IllegalStateException("Native dataset control requires a durable catalog");
        }, "lance_catalog", "s3://warehouse");
        assertThatThrownBy(() -> manager.getSplits(ConnectorTransactionHandle.EMPTY, session("tenant-b"), pinned,
                DynamicFilter.ALL, Constraint.EMPTY)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> manager.getSplits(ConnectorTransactionHandle.EMPTY, session("tenant-a"), pinned,
                DynamicFilter.ALL, Constraint.EMPTY)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Native dataset control");
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
