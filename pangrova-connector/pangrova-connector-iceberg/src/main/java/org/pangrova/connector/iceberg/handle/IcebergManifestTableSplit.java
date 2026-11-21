package org.pangrova.connector.iceberg.handle;

import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.connector.api.metadata.ConnectorSplit;

/** One SHA-pinned Iceberg manifest-list object scanned by the Native Worker. */
public record IcebergManifestTableSplit(IcebergManifestTableHandle selection) implements ConnectorSplit {
    public IcebergManifestTableSplit {
        Objects.requireNonNull(selection, "selection");
        if (selection.empty()) {
            throw new IllegalArgumentException("An empty Iceberg manifests selection has no split");
        }
    }

    @Override
    public Map<String, String> splitProperties() {
        return Map.of(
                "file_path", selection.manifestListLocation(),
                "file_format", "metadata_avro",
                "start_offset", "0",
                "length", Long.toString(selection.manifestListLength()),
                "connector_id", "iceberg");
    }

    @Override
    public byte[] toSplitInfoProto() {
        var system = SystemTableSplitDetail.newBuilder()
                .setKind(SystemTableSplitDetail.Kind.ICEBERG_MANIFESTS)
                .setSnapshotId(selection.snapshotId().orElseThrow())
                .setObjectSha256(com.google.protobuf.ByteString.copyFrom(
                        HexFormat.of().parseHex(selection.manifestListSha256())))
                .setIcebergFormatVersion(selection.baseTable().formatVersion())
                .setIcebergSnapshotSequenceNumber(selection.snapshotSequenceNumber());
        system.addAllIcebergPartitionSpecs(selection.protocolPartitionSpecs());
        FileSplitDetail detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_METADATA_AVRO)
                .setSystemTable(system)
                .build();
        String path = selection.manifestListLocation();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", selection.tableName(), path, path,
                path, 0, selection.manifestListLength(), Map.of(), detail)).toByteArray();
    }
}
