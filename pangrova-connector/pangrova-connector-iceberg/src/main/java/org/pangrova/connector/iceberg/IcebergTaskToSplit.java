package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergDeleteFile;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts Iceberg {@link FileScanTask}s into {@link IcebergSplit}s, including
 * partition value extraction, delete-file descriptors, and residual predicates.
 */
final class IcebergTaskToSplit {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergTaskToSplit.class);

    private final IcebergExpressionToSubstrait expressionConverter;

    IcebergTaskToSplit(IcebergExpressionToSubstrait expressionConverter) {
        this.expressionConverter = expressionConverter;
    }

    /** Converts one planned file scan task into an executable split. */
    IcebergSplit convert(FileScanTask task, String qualifiedTableName,
                         String readMode, long snapshotId, long fromSnapshotId, long toSnapshotId,
                         Schema tableSchema, List<ColumnMapping> columnMappings) {
        byte[] residualPredicate = expressionConverter.convert(task.residual(), tableSchema)
                .orElse(new byte[0]);
        long dataSeqNum = task.file().dataSequenceNumber() != null
                ? task.file().dataSequenceNumber() : 0L;
        return IcebergSplit.builder(
                        task.file().path().toString(),
                        task.start(),
                        task.length(),
                        task.file().format().name())
                .tableName(qualifiedTableName)
                .dataFileSizeBytes(task.file().fileSizeInBytes())
                .partitionValues(extractPartitionValues(task))
                .snapshotId(snapshotId)
                .dataSequenceNumber(dataSeqNum)
                .specId(task.spec().specId())
                .content("DATA")
                .readMode(readMode)
                .fromSnapshotId(fromSnapshotId)
                .toSnapshotId(toSnapshotId)
                .deleteFiles(extractTypedDeleteFiles(task))
                .columnMappings(columnMappings)
                .residualPredicate(residualPredicate)
                .build();
    }

    /**
     * Extracts partition values from a FileScanTask, evaluating hidden partition
     * transforms (year/month/day/hour/bucket/truncate) on the Java side.
     *
     * <p>For identity partitions, the source column name is used as the key.
     * For hidden transforms, the partition field name (e.g., "date_year") is used
     * as the key, and the transform-evaluated value is already available in the
     * StructLike partition data from the Iceberg SDK.</p>
     *
     * <p>Validates: Requirements R4.2, R4.3</p>
     */
    private Map<String, String> extractPartitionValues(FileScanTask task) {
        StructLike partition = task.file().partition();
        PartitionSpec spec = task.spec();
        if (partition.size() == 0) {
            return Map.of();
        }
        List<PartitionField> fields = spec.fields();
        Map<String, String> values = new LinkedHashMap<>();
        int limit = Math.min(partition.size(), fields.size());
        for (int i = 0; i < limit; i++) {
            PartitionField field = fields.get(i);
            Object value = partition.get(i, Object.class);
            values.put(field.name(), value == null ? "null" : value.toString());
            logHiddenTransform(field, spec.schema(), value);
        }
        return values;
    }

    private void logHiddenTransform(PartitionField field, Schema schema, Object value) {
        if (field.transform().isIdentity()) {
            return;
        }
        Types.NestedField sourceField = schema.findField(field.sourceId());
        if (sourceField != null) {
            LOG.debug("event=hidden_partition_evaluated field={} source_column={} transform={} value={}",
                    field.name(), sourceField.name(), field.transform(),
                    value == null ? "null" : value.toString());
        }
    }

    /**
     * Extracts typed delete file descriptors from a FileScanTask.
     *
     * <p>Maps Iceberg SDK DeleteFile objects to IcebergDeleteFile records,
     * preserving delete type, equality fields, and sequence numbers.</p>
     */
    private List<IcebergDeleteFile> extractTypedDeleteFiles(FileScanTask task) {
        if (task.deletes().isEmpty()) {
            return List.of();
        }
        List<IcebergDeleteFile> result = new ArrayList<>();
        for (DeleteFile deleteFile : task.deletes()) {
            String deleteType = resolveDeleteType(deleteFile);
            List<String> equalityFields = extractEqualityFields(deleteFile, task);
            String dvFilePath = "DELETION_VECTOR".equals(deleteType)
                    ? deleteFile.path().toString() : "";
            result.add(new IcebergDeleteFile(
                    deleteFile.path().toString(),
                    deleteType,
                    equalityFields,
                    deleteFile.recordCount(),
                    deleteFile.dataSequenceNumber(),
                    dvFilePath));
        }
        return result;
    }

    private static String resolveDeleteType(DeleteFile deleteFile) {
        if (deleteFile.format() != null
                && "puffin".equalsIgnoreCase(deleteFile.format().name())) {
            return "DELETION_VECTOR";
        }
        return switch (deleteFile.content()) {
            case POSITION_DELETES -> "POSITION";
            case EQUALITY_DELETES -> "EQUALITY";
            default -> "POSITION";
        };
    }

    private List<String> extractEqualityFields(DeleteFile deleteFile, FileScanTask task) {
        if (deleteFile.equalityFieldIds() == null || deleteFile.equalityFieldIds().isEmpty()) {
            return List.of();
        }
        Schema schema = task.spec().schema();
        List<String> fieldNames = new ArrayList<>();
        for (int fieldId : deleteFile.equalityFieldIds()) {
            Types.NestedField field = schema.findField(fieldId);
            if (field != null) {
                fieldNames.add(field.name());
            }
        }
        return fieldNames;
    }

    /**
     * Computes column name mappings for schema evolution.
     *
     * <p>Compares all historical schemas with the current schema by field ID.
     * When a column has been renamed (same field ID, different name), a
     * {@link ColumnMapping} is produced so the native reader can find the
     * physical column in older Parquet files by its historical name.</p>
     */
    static List<ColumnMapping> computeColumnMappings(Table table) {
        Schema currentSchema = table.schema();
        Map<Integer, String> currentFieldNames = new HashMap<>();
        for (Types.NestedField field : currentSchema.columns()) {
            currentFieldNames.put(field.fieldId(), field.name());
        }

        Set<String> seenMappingKeys = new HashSet<>();
        List<ColumnMapping> mappings = new ArrayList<>();

        for (Schema oldSchema : table.schemas().values()) {
            if (oldSchema.schemaId() == currentSchema.schemaId()) {
                continue;
            }
            for (Types.NestedField oldField : oldSchema.columns()) {
                String currentName = currentFieldNames.get(oldField.fieldId());
                if (currentName != null && !currentName.equals(oldField.name())) {
                    String key = oldField.fieldId() + ":" + oldField.name();
                    if (seenMappingKeys.add(key)) {
                        mappings.add(new ColumnMapping(
                                oldField.fieldId(), oldField.fieldId(),
                                oldField.name(), currentName));
                    }
                }
            }
        }
        return mappings;
    }
}
