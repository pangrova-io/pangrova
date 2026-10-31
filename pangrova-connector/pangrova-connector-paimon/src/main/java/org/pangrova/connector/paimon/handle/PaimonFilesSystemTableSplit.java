package org.pangrova.connector.paimon.handle;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.connector.spi.metadata.ConnectorSplit;

import java.util.HexFormat;
import java.util.Map;

/** SDK-selected immutable manifest read by the Native metadata operator. */
public record PaimonFilesSystemTableSplit(String tableName,
        PaimonFilesTableHandle.Manifest manifest) implements ConnectorSplit {
    @Override
    public Map<String, String> splitProperties() {
        return Map.of("file_path", manifest.uri(), "file_format", "metadata_avro",
                "start_offset", "0", "length", Long.toString(manifest.length()), "connector_id", "paimon");
    }

    @Override
    public byte[] toSplitInfoProto() {
        var detail = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_METADATA_AVRO)
                .setSystemTable(SystemTableSplitDetail.newBuilder().setKind(SystemTableSplitDetail.Kind.PAIMON_FILES)
                        .setSnapshotId(manifest.snapshotId())
                        .setObjectSha256(ByteString.copyFrom(HexFormat.of().parseHex(manifest.sha256())))
                        .setPaimonFiles(manifest.context())).build();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "paimon", tableName, manifest.context().getSnapshot().getUri(), manifest.uri(), manifest.uri(),
                0, manifest.length(), Map.of(), detail)).toByteArray();
    }
}
