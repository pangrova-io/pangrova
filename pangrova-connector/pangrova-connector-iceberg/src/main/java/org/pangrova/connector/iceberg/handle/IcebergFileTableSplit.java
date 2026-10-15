package org.pangrova.connector.iceberg.handle;

import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.metadata.ConnectorSplit;

/** One child manifest whose parent list and own bytes are pinned for a Native $files scan. */
public record IcebergFileTableSplit(IcebergFileTableHandle selection,
        IcebergFileTableHandle.ManifestObject manifest) implements ConnectorSplit {
    public IcebergFileTableSplit {
        Objects.requireNonNull(selection, "selection");
        Objects.requireNonNull(manifest, "manifest");
        if (!selection.manifests().contains(manifest) || selection.manifestSelection().empty()) {
            throw new IllegalArgumentException("Child manifest is not in selected snapshot");
        }
    }

    @Override
    public Map<String, String> splitProperties() {
        return Map.of(
                "file_path", manifest.uri(),
                "file_format", "metadata_avro",
                "start_offset", "0",
                "length", Long.toString(manifest.length()),
                "connector_id", "iceberg");
    }

    @Override
    public byte[] toSplitInfoProto() {
        var list = selection.manifestSelection();
        var system = SystemTableSplitDetail.newBuilder()
                .setKind(SystemTableSplitDetail.Kind.ICEBERG_FILES)
                .setSnapshotId(list.snapshotId().orElseThrow())
                .setObjectSha256(com.google.protobuf.ByteString.copyFrom(
                        HexFormat.of().parseHex(manifest.sha256())))
                .setIcebergFormatVersion(list.baseTable().formatVersion())
                .setIcebergSnapshotSequenceNumber(list.snapshotSequenceNumber())
                .setIcebergManifestListUri(list.manifestListLocation())
                .setIcebergManifestListLength(list.manifestListLength())
                .setIcebergManifestListSha256(com.google.protobuf.ByteString.copyFrom(
                        HexFormat.of().parseHex(list.manifestListSha256())))
                .setIcebergChildSpecId(manifest.specId())
                .setIcebergChildContent(manifest.content());
        FileSplitDetail detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_METADATA_AVRO)
                .setSystemTable(system)
                .build();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", selection.tableName(), list.manifestListLocation(),
                manifest.uri(), manifest.uri(), 0,
                manifest.length(), Map.of(), detail)).toByteArray();
    }
}
