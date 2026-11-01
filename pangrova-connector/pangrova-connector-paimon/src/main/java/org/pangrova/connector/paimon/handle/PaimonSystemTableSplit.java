package org.pangrova.connector.paimon.handle;

import java.util.Map;
import java.util.HexFormat;

import com.google.protobuf.ByteString;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.metadata.ConnectorSplit;

/** One pinned Paimon snapshot JSON object read by a Native Worker. */
public record PaimonSystemTableSplit(
        String tableName,
        String sourceIdentity,
        PaimonSystemTableHandle.Kind kind,
        PaimonSystemTableHandle.MetadataObject object) implements ConnectorSplit {

    @Override
    public Map<String, String> splitProperties() {
        return Map.of(
                "file_path", object.uri(),
                "file_format", "metadata_json",
                "start_offset", "0",
                "length", Long.toString(object.length()),
                "connector_id", "paimon");
    }

    @Override
    public byte[] toSplitInfoProto() {
        SystemTableSplitDetail.Kind protocolKind = switch (kind) {
            case SNAPSHOTS -> SystemTableSplitDetail.Kind.PAIMON_SNAPSHOTS;
            case TAGS -> SystemTableSplitDetail.Kind.PAIMON_TAGS;
            case OPTIONS -> SystemTableSplitDetail.Kind.PAIMON_OPTIONS;
            case SCHEMAS -> SystemTableSplitDetail.Kind.PAIMON_SCHEMAS;
        };
        var detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_METADATA_JSON)
                .setSystemTable(SystemTableSplitDetail.newBuilder()
                        .setKind(protocolKind)
                        .setSnapshotId(object.snapshotId())
                        .setSchemaId(object.schemaId() < 0 ? 0 : object.schemaId())
                        .setObjectSha256(ByteString.copyFrom(HexFormat.of().parseHex(object.sha256())))
                        .setTagName(object.tagName() == null ? "" : object.tagName())
                        .build())
                .build();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "paimon", tableName, sourceIdentity, object.uri(), object.uri(),
                0, object.length(), Map.of(), detail)).toByteArray();
    }
}
