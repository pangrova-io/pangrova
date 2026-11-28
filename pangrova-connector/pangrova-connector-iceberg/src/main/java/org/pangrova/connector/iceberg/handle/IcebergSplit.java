package org.pangrova.connector.iceberg.handle;

import java.util.Locale;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DeleteFileDetail;
import org.pangrova.protocol.v2.DeleteFileType;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IcebergReadMode;
import org.pangrova.protocol.v2.IcebergSplitDetail;
import org.pangrova.protocol.v2.SchemaEvolution;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.connector.api.metadata.SplitConstraint;
import org.pangrova.connector.api.metadata.table.ColumnMapping;
import org.pangrova.connector.api.metadata.table.TypePromotion;

/**
 * Split representing a single Iceberg file scan task.
 *
 * @param tableName            logical table name for split routing in multi-table joins
 * @param filePath             data file path
 * @param fileOffset           byte offset within the file
 * @param fileLength           byte length of the split
 * @param dataFileSizeBytes    whole data object length from the selected snapshot
 * @param fileFormat           file format (e.g. "PARQUET", "ORC")
 * @param partitionValues      partition column name-value pairs
 * @param snapshotId           snapshot id this split belongs to
 * @param dataSequenceNumber   data sequence number of the data file
 * @param specId               partition spec id
 * @param content              file content type: "DATA", "POSITION_DELETES", "EQUALITY_DELETES"
 * @param readMode             read mode: "FULL" or "INCREMENTAL"
 * @param fromSnapshotId       start snapshot for incremental reads (0 if not applicable)
 * @param toSnapshotId         end snapshot for incremental reads (0 if not applicable)
 * @param deleteFiles          typed delete file descriptors for v2 delete operations
 * @param columnMappings       schema evolution column mappings (source → target by field ID)
 * @param typePromotions       schema evolution type promotions (e.g., int → long)
 * @param residualPredicate    serialized Substrait expression from FileScanTask.residual for native row-group skip
 * @param estimatedScanBytes   planned data range and auxiliary delete bytes for Coordinator placement
 * @param estimatedScanRows    planned data rows for Coordinator placement
 */
public record IcebergSplit(
        String tableName,
        String filePath,
        long fileOffset,
        long fileLength,
        long dataFileSizeBytes,
        String fileFormat,
        Map<String, String> partitionValues,
        long snapshotId,
        long dataSequenceNumber,
        int specId,
        String content,
        String readMode,
        long fromSnapshotId,
        long toSnapshotId,
        List<IcebergDeleteFile> deleteFiles,
        List<ColumnMapping> columnMappings,
        List<TypePromotion> typePromotions,
        byte[] residualPredicate,
        long estimatedScanBytes,
        long estimatedScanRows) implements CanonicalConnectorSplit {

    public IcebergSplit {
        if (fileFormat != null && "LANCE".equalsIgnoreCase(fileFormat)) {
            throw new IllegalArgumentException(
                    "Iceberg does not expose Lance as a supported data-file format");
        }
        if (dataFileSizeBytes < 0 || (dataFileSizeBytes > 0
                && (fileOffset < 0 || fileLength <= 0 || fileOffset > dataFileSizeBytes
                        || fileLength > dataFileSizeBytes - fileOffset))) {
            throw new IllegalArgumentException("Iceberg split range exceeds its snapshot data object");
        }
        tableName = tableName == null ? "" : tableName;
        partitionValues = partitionValues == null ? Map.of() : Map.copyOf(partitionValues);
        deleteFiles = deleteFiles == null ? List.of() : List.copyOf(deleteFiles);
        columnMappings = columnMappings == null ? List.of() : List.copyOf(columnMappings);
        typePromotions = typePromotions == null ? List.of() : List.copyOf(typePromotions);
        residualPredicate = residualPredicate == null
                ? new byte[0]
                : Arrays.copyOf(residualPredicate, residualPredicate.length);
        estimatedScanBytes = Math.max(Math.max(0L, fileLength), estimatedScanBytes);
        estimatedScanRows = Math.max(0L, estimatedScanRows);
    }

    /** Creates a physical subrange of the same immutable Parquet object. */
    public IcebergSplit withRange(long offset, long length) {
        if (offset < fileOffset || length <= 0 || offset - fileOffset > fileLength - length) {
            throw new IllegalArgumentException("Parquet range falls outside the planned file interval");
        }
        long auxiliaryBytes = estimatedScanBytes - fileLength;
        long rows = Math.round(estimatedScanRows * ((double) length / fileLength));
        return new IcebergSplit(tableName, filePath, offset, length, dataFileSizeBytes, fileFormat,
                partitionValues, snapshotId, dataSequenceNumber, specId, content, readMode,
                fromSnapshotId, toSnapshotId, deleteFiles, columnMappings, typePromotions, residualPredicate,
                Math.addExact(length, auxiliaryBytes), rows);
    }

    @Override
    public byte[] residualPredicate() {
        return Arrays.copyOf(residualPredicate, residualPredicate.length);
    }

    /**
     * Creates a new {@link Builder} for constructing IcebergSplit instances.
     * Preferred over telescoping constructors for readability.
     */
    public static Builder builder(String filePath, long fileOffset, long fileLength, String fileFormat) {
        return new Builder(filePath, fileOffset, fileLength, fileFormat);
    }

    /**
     * Builder for {@link IcebergSplit} with sensible defaults for optional fields.
     */
    public static final class Builder {
        private final String filePath;
        private final long fileOffset;
        private final long fileLength;
        private final String fileFormat;

        private String tableName = "";
        private long dataFileSizeBytes = 0L;
        private Map<String, String> partitionValues = Map.of();
        private long snapshotId = 0L;
        private long dataSequenceNumber = 0L;
        private int specId = 0;
        private String content = "DATA";
        private String readMode = "FULL";
        private long fromSnapshotId = 0L;
        private long toSnapshotId = 0L;
        private List<IcebergDeleteFile> deleteFiles = List.of();
        private List<ColumnMapping> columnMappings = List.of();
        private List<TypePromotion> typePromotions = List.of();
        private byte[] residualPredicate = new byte[0];
        private long estimatedScanBytes;
        private long estimatedScanRows;

        private Builder(String filePath, long fileOffset, long fileLength, String fileFormat) {
            this.filePath = filePath;
            this.fileOffset = fileOffset;
            this.fileLength = fileLength;
            this.fileFormat = fileFormat;
        }

        public Builder tableName(String table) {
            this.tableName = table;
            return this;
        }
        public Builder dataFileSizeBytes(long sizeBytes) {
            this.dataFileSizeBytes = sizeBytes;
            return this;
        }
        public Builder partitionValues(Map<String, String> partitions) {
            this.partitionValues = partitions;
            return this;
        }
        public Builder snapshotId(long snapshot) {
            this.snapshotId = snapshot;
            return this;
        }
        public Builder dataSequenceNumber(long sequenceNumber) {
            this.dataSequenceNumber = sequenceNumber;
            return this;
        }
        public Builder specId(int spec) {
            this.specId = spec;
            return this;
        }
        public Builder content(String fileContent) {
            this.content = fileContent;
            return this;
        }
        public Builder readMode(String mode) {
            this.readMode = mode;
            return this;
        }
        public Builder fromSnapshotId(long startSnapshotId) {
            this.fromSnapshotId = startSnapshotId;
            return this;
        }
        public Builder toSnapshotId(long endSnapshotId) {
            this.toSnapshotId = endSnapshotId;
            return this;
        }
        public Builder deleteFiles(List<IcebergDeleteFile> deletes) {
            this.deleteFiles = deletes;
            return this;
        }
        public Builder columnMappings(List<ColumnMapping> mappings) {
            this.columnMappings = mappings;
            return this;
        }
        public Builder typePromotions(List<TypePromotion> promotions) {
            this.typePromotions = promotions;
            return this;
        }
        public Builder residualPredicate(byte[] predicate) {
            this.residualPredicate = predicate;
            return this;
        }

        public Builder scanEstimate(long bytes, long rows) {
            this.estimatedScanBytes = bytes;
            this.estimatedScanRows = rows;
            return this;
        }

        /**
         * Builds an immutable {@link IcebergSplit} from the current builder state.
         */
        public IcebergSplit build() {
            return new IcebergSplit(
                    tableName, filePath, fileOffset, fileLength, dataFileSizeBytes,
                    fileFormat, partitionValues, snapshotId, dataSequenceNumber,
                    specId, content, readMode, fromSnapshotId, toSnapshotId,
                    deleteFiles, columnMappings, typePromotions, residualPredicate,
                    estimatedScanBytes, estimatedScanRows);
        }
    }

    @Override
    public SplitConstraint splitConstraint() {
        if (partitionValues.isEmpty()) {
            return SplitConstraint.NONE;
        }
        return new SplitConstraint(partitionValues, Map.of());
    }

    @Override
    public Map<String, String> splitProperties() {
        // PERF-T11: Use Map.of() directly instead of LinkedHashMap + Map.copyOf().
        // Eliminates intermediate LinkedHashMap allocation for this immutable record.
        return Map.of(
                "file_path", filePath,
                "file_format", fileFormat != null ? fileFormat.toLowerCase(Locale.ROOT) : "parquet",
                "start_offset", String.valueOf(fileOffset),
                "length", String.valueOf(fileLength),
                "connector_id", "iceberg");
    }

    /**
     * Builds a canonical v2 DataSplit with a typed FileSplitDetail extension.
     *
     * <p>Directly constructs the SplitInfo + IcebergSplitInfo proto messages,
     * including delete_files (with DV support), partition_values, and
     * schema_evolution (column_mappings + type_promotions).</p>
     */
    @Override
    public DataSplit toDataSplit() {
        FileSplitDetail.Builder detail = FileSplitDetail.newBuilder()
                .setFileFormat(resolveFileFormat())
                .setIceberg(buildIcebergSplitDetail());

        buildSchemaEvolution(detail);
        buildSplitConstraint(detail);

        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", tableName, Long.toString(snapshotId), filePath, filePath,
                fileOffset, fileLength, partitionValues, detail.build()));
    }

    private IcebergSplitDetail buildIcebergSplitDetail() {
        IcebergSplitDetail.Builder builder = IcebergSplitDetail.newBuilder()
                .setReadMode(resolveReadMode())
                .setSnapshotId(snapshotId)
                .setDataSequenceNumber(dataSequenceNumber)
                .setSpecId(specId)
                .setContent(resolveContent())
                .setDataFileSizeBytes(dataFileSizeBytes)
                .setFromSnapshotId(fromSnapshotId)
                .setToSnapshotId(toSnapshotId);

        for (IcebergDeleteFile df : deleteFiles) {
            builder.addDeleteFiles(buildDeleteFileDetail(df));
        }

        return builder.build();
    }

    private static DeleteFileDetail buildDeleteFileDetail(IcebergDeleteFile df) {
        DeleteFileDetail.Builder builder = DeleteFileDetail.newBuilder()
                .setOpaqueLocation(df.filePath())
                .setDeleteType(resolveDeleteFileType(df.deleteType()))
                .setRecordCount(df.recordCount())
                .setDataSequenceNumber(df.dataSequenceNumber());

        builder.addAllEqualityFields(df.equalityFields());

        if (!df.dvFilePath().isEmpty()) {
            builder.setDeletionVectorLocation(df.dvFilePath());
        }

        return builder.build();
    }

    private void buildSchemaEvolution(FileSplitDetail.Builder splitBuilder) {
        if (columnMappings.isEmpty() && typePromotions.isEmpty()) {
            return;
        }
        SchemaEvolution.Builder evoBuilder = SchemaEvolution.newBuilder();
        addColumnMappingsTo(evoBuilder);
        addTypePromotionsTo(evoBuilder);
        splitBuilder.setSchemaEvolution(evoBuilder.build());
    }

    private void buildSplitConstraint(FileSplitDetail.Builder splitBuilder) {
        if (residualPredicate.length == 0) {
            return;
        }
        org.pangrova.protocol.v2.SplitConstraint.Builder constraintBuilder =
                org.pangrova.protocol.v2.SplitConstraint.newBuilder()
                .setPredicate(ByteString.copyFrom(residualPredicate));
        splitBuilder.setSplitConstraint(constraintBuilder.build());
    }

    private void addColumnMappingsTo(SchemaEvolution.Builder evoBuilder) {
        for (ColumnMapping cm : columnMappings) {
            evoBuilder.addColumnMappings(
                    org.pangrova.protocol.v2.ColumnMapping.newBuilder()
                            .setSourceFieldId(cm.sourceFieldId())
                            .setTargetFieldId(cm.targetFieldId())
                            .setSourceName(cm.sourceName())
                            .setTargetName(cm.targetName())
                            .build());
        }
    }

    private void addTypePromotionsTo(SchemaEvolution.Builder evoBuilder) {
        for (TypePromotion tp : typePromotions) {
            var builder = org.pangrova.protocol.v2.TypePromotion.newBuilder()
                    .setFieldId(tp.fieldId())
                    .setSourceType(tp.sourceType())
                    .setTargetType(tp.targetType());
            if (!tp.columnName().isEmpty()) {
                builder.setColumnName(tp.columnName());
            }
            evoBuilder.addTypePromotions(builder.build());
        }
    }

    private FileFormat resolveFileFormat() {
        if (fileFormat == null) {
            return FileFormat.FILE_FORMAT_PARQUET;
        }
        return switch (fileFormat.toUpperCase(Locale.ROOT)) {
            case "ORC" -> FileFormat.FILE_FORMAT_ORC;
            default -> FileFormat.FILE_FORMAT_PARQUET;
        };
    }

    private IcebergReadMode resolveReadMode() {
        if (readMode == null) {
            return IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT;
        }
        return switch (readMode.toUpperCase(Locale.ROOT)) {
            case "INCREMENTAL" -> IcebergReadMode.ICEBERG_READ_MODE_INCREMENTAL;
            // Snapshot selection is a table-handle concern. Once pinned, FULL
            // and TIME_TRAVEL describe the same physical scan and must carry
            // identical durable split evidence for the same snapshot.
            case "FULL", "TIME_TRAVEL" -> IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT;
            default -> IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT;
        };
    }

    private int resolveContent() {
        if (content == null) {
            return 0;
        }
        return switch (content.toUpperCase(Locale.ROOT)) {
            case "POSITION_DELETES" -> 1;
            case "EQUALITY_DELETES" -> 2;
            default -> 0;
        };
    }

    private static DeleteFileType resolveDeleteFileType(String deleteType) {
        return switch (deleteType.toUpperCase(Locale.ROOT)) {
            case "POSITION" -> DeleteFileType.DELETE_FILE_TYPE_POSITION;
            case "EQUALITY" -> DeleteFileType.DELETE_FILE_TYPE_EQUALITY;
            case "DELETION_VECTOR" -> DeleteFileType.DELETE_FILE_TYPE_DELETION_VECTOR;
            default -> DeleteFileType.DELETE_FILE_TYPE_UNSPECIFIED;
        };
    }
}
