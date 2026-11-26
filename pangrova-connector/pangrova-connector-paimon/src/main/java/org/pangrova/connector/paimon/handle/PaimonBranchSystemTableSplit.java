package org.pangrova.connector.paimon.handle;

import java.util.Map;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.protocol.v2.DataSplit;

/** One pinned local branch directory; Native reads its own directory status. */
public record PaimonBranchSystemTableSplit(
        String tableName,
        String sourceIdentity,
        PaimonBranchTableHandle.BranchDirectory directory) implements CanonicalConnectorSplit {

    @Override
    public Map<String, String> splitProperties() {
        return Map.of(
                "file_path", directory.uri(),
                "file_format", "metadata_directory",
                "start_offset", "0",
                "length", "0",
                "connector_id", "paimon");
    }

    @Override
    public DataSplit toDataSplit() {
        var detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_METADATA_DIRECTORY)
                .setSystemTable(SystemTableSplitDetail.newBuilder()
                        .setKind(SystemTableSplitDetail.Kind.PAIMON_BRANCHES)
                        .setBranchName(directory.name())
                        .setDirectoryMtimeMs(directory.modificationTimeMillis())
                        .setDirectoryDevice(directory.device())
                        .setDirectoryInode(directory.inode())
                        .setDirectoryZoneOffsetSeconds(directory.zoneOffsetSeconds())
                        .build())
                .build();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "paimon", tableName, sourceIdentity, directory.uri(), directory.uri(),
                0, 0, Map.of(), detail));
    }
}
