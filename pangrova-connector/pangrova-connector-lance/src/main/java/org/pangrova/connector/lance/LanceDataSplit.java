package org.pangrova.connector.lance;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ScanPartitionDetail;
import org.pangrova.spi.connector.metadata.NativeDatasetScanPage;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import java.util.Map;

/** Canonical dataset split pinned to the catalog-visible Lance version. */
public record LanceDataSplit(LanceTableHandle table, NativeDatasetScanPage.Partition partition,
        byte[] serializedCatalogBinding, byte[] manifestSha256) implements ConnectorSplit {
    public LanceDataSplit {
        if (table.version() <= 0) throw new IllegalArgumentException("Lance scan requires a published version");
        serializedCatalogBinding = serializedCatalogBinding.clone();
        manifestSha256 = manifestSha256.clone();
    }
    @Override public Map<String, String> splitProperties() {
        return Map.of("file_path", table.datasetUri(), "file_format", "lance", "connector_id", "lance");
    }
    @Override public byte[] toSplitInfoProto() {
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "lance", table.catalog() + "." + table.table(), Long.toString(table.version()),
                table.tableId() + ":" + table.version() + ":" + partition.id(), table.datasetUri(), 0, 0, Map.of(),
                FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_LANCE)
                        .setScanPartition(ScanPartitionDetail.newBuilder().setPartitionId(partition.id())
                                .setPhysicalRows(partition.physicalRows()).setManifestSha256(checksum(manifestSha256))
                                .setPhysicalSchemaSha256(checksum(table.schemaFence()))).build()))
                .toBuilder().setCatalog(catalogBinding()).build().toByteArray();
    }

    private CatalogExecutionBinding catalogBinding() {
        try {
            return CatalogExecutionBinding.parseFrom(serializedCatalogBinding);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Invalid scan catalog identity", malformed);
        }
    }
    private static Checksum checksum(byte[] digest) {
        if (digest.length != 32) throw new IllegalArgumentException("Scan split requires SHA-256 evidence");
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(com.google.protobuf.ByteString.copyFrom(digest)).build();
    }
    @Override public byte[] serializedCatalogBinding() { return serializedCatalogBinding.clone(); }
    @Override public byte[] manifestSha256() { return manifestSha256.clone(); }
}
