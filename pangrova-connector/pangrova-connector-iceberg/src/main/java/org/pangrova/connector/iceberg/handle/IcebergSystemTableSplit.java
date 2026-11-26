package org.pangrova.connector.iceberg.handle;

import java.util.Map;
import java.util.Objects;

import org.pangrova.connector.iceberg.enums.IcebergSystemTableKind;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.protocol.v2.DataSplit;

/** One pinned Iceberg metadata object scanned by a Native Worker. */
public record IcebergSystemTableSplit(
        String tableName,
        IcebergSystemTableKind kind,
        String metadataLocation,
        long metadataLength) implements CanonicalConnectorSplit {

    public IcebergSystemTableSplit {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must be nonblank");
        }
        Objects.requireNonNull(kind, "kind");
        if (metadataLocation == null || metadataLocation.isBlank()) {
            throw new IllegalArgumentException("metadataLocation must be nonblank");
        }
        if (metadataLength <= 0) {
            throw new IllegalArgumentException("metadataLength must be positive");
        }
    }

    @Override
    public Map<String, String> splitProperties() {
        return Map.of(
                "file_path", metadataLocation,
                "file_format", "metadata_json",
                "start_offset", "0",
                "length", Long.toString(metadataLength),
                "connector_id", "iceberg");
    }

    @Override
    public DataSplit toDataSplit() {
        SystemTableSplitDetail.Kind protocolKind = switch (kind) {
            case SNAPSHOTS -> SystemTableSplitDetail.Kind.ICEBERG_SNAPSHOTS;
            case HISTORY -> SystemTableSplitDetail.Kind.ICEBERG_HISTORY;
            case REFS -> SystemTableSplitDetail.Kind.ICEBERG_REFS;
        };
        FileSplitDetail detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_METADATA_JSON)
                .setSystemTable(SystemTableSplitDetail.newBuilder()
                        .setKind(protocolKind)
                        .build())
                .build();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", tableName, metadataLocation, metadataLocation,
                metadataLocation, 0, metadataLength, Map.of(), detail));
    }
}
