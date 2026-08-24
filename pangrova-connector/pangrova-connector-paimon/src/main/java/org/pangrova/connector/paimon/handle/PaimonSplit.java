package org.pangrova.connector.paimon.handle;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.LsmLevelInfo;
import org.pangrova.protocol.v1.PaimonChangelogProducer;
import org.pangrova.protocol.v1.PaimonMergeEngine;
import org.pangrova.protocol.v1.PaimonReadMode;
import org.pangrova.protocol.v1.PaimonSplitInfo;
import org.pangrova.protocol.v1.SchemaEvolution;
import org.pangrova.protocol.v1.SplitConstraintInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.SplitConstraint;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;

/**
 * Paimon split descriptor for data reading.
 *
 * <p><b>Design note — SDK Split isolation:</b> The Paimon SDK {@code Split} object is
 * <em>not</em> stored in this record. Instead, it is held in a {@code PaimonSplitRegistry}
 * keyed by {@link #splitRegistryId()}. This prevents SDK type leakage across module
 * boundaries (the {@code ConnectorSplit} SPI is visible to coordinator and worker).
 * {@code PaimonPageSourceProvider} consumes the SDK Split from the registry at read time.</p>
 *
 * @param tableName                table name this split belongs to
 * @param splitRegistryId          key into PaimonSplitRegistry for the SDK Split object
 * @param rowCount                 total row count for this split (extracted from SDK Split at planning time)
 * @param bucketId                 bucket id (-1 if not bucketed)
 * @param primaryKeys              primary key column names
 * @param mergeEngine              merge engine type (e.g. "DEDUPLICATE", "PARTIAL_UPDATE")
 * @param changelogProducer        changelog producer type (e.g. "NONE", "INPUT", "LOOKUP")
 * @param snapshotId               snapshot id this split belongs to
 * @param fromSnapshotId           start snapshot for incremental reads (0 if not applicable)
 * @param toSnapshotId             end snapshot for incremental reads (0 if not applicable)
 * @param readMode                 read mode: "FULL" or "INCREMENTAL"
 * @param lsmLevels                LSM level descriptors for level-aware merge
 * @param needsMerge               true if MOR merge is required at read time
 * @param deletionVectorsEnabled   true if MOW deletion vectors are present
 * @param deletionVectorFilePath   path to the deletion vector file (empty if not applicable)
 * @param aggregationSpecs         column name → aggregation function mapping for AGGREGATION engine
 * @param changelogMode            true when reading changelog files with _row_kind column
 * @param partitionValues          partition column name-value pairs
 * @param columnMappings           schema evolution column mappings (source → target by field ID)
 * @param typePromotions           schema evolution type promotions (e.g., int → long)
 * @param primaryFilePath          physical file path for COW/read-optimized reads (first data file)
 */
public record PaimonSplit(
        String tableName,
        long splitRegistryId,
        long rowCount,
        int bucketId,
        List<String> primaryKeys,
        String mergeEngine,
        String changelogProducer,
        long snapshotId,
        long fromSnapshotId,
        long toSnapshotId,
        String readMode,
        List<PaimonLsmLevel> lsmLevels,
        boolean needsMerge,
        boolean deletionVectorsEnabled,
        String deletionVectorFilePath,
        Map<String, String> aggregationSpecs,
        boolean changelogMode,
        Map<String, String> partitionValues,
        List<ColumnMapping> columnMappings,
        List<TypePromotion> typePromotions,
        String primaryFilePath) implements ConnectorSplit {

    public PaimonSplit {
        primaryKeys = copyOf(primaryKeys);
        lsmLevels = copyOf(lsmLevels);
        deletionVectorFilePath = emptyIfNull(deletionVectorFilePath);
        aggregationSpecs = copyOf(aggregationSpecs);
        partitionValues = copyOf(partitionValues);
        columnMappings = copyOf(columnMappings);
        typePromotions = copyOf(typePromotions);
        primaryFilePath = emptyIfNull(primaryFilePath);
    }

    private static <T> List<T> copyOf(List<T> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static <K, V> Map<K, V> copyOf(Map<K, V> values) {
        return values == null ? Map.of() : Map.copyOf(values);
    }

    private static String emptyIfNull(String value) {
        return value == null ? "" : value;
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
        return Map.of(
                "connector_id", "paimon",
                "file_format", resolveFileFormatString(),
                "file_path", resolveFilePath(),
                "start_offset", "0",
                "byte_length", String.valueOf(resolveByteLength()),
                "row_count", String.valueOf(rowCount));
    }

    /**
     * Builds a serialized SplitInfo proto for native engine dispatch.
     *
     * <p>Directly constructs the SplitInfo + PaimonSplitInfo proto messages,
     * including needs_merge, deletion_vectors_enabled, aggregation_specs,
     * changelog_mode, partition_values, and schema_evolution.</p>
     *
     * <p>Validates: Requirements R14.5, R19.3</p>
     */
    @Override
    public byte[] toSplitInfoProto() {
        SplitInfo.Builder splitBuilder = SplitInfo.newBuilder()
                .setConnectorId("paimon")
                .setFilePath(resolveFilePath())
                .setFileFormat(resolveFileFormat())
                .setStartOffset(0L)
                .setLength(rowCount)
                .setTableName(tableName)
                .setPaimonInfo(buildPaimonSplitInfo());

        buildSchemaEvolution(splitBuilder);
        buildSplitConstraint(splitBuilder);

        return splitBuilder.build().toByteArray();
    }

    private PaimonSplitInfo buildPaimonSplitInfo() {
        // Filter out null/empty primary keys defensively.
        List<String> normalizedKeys = primaryKeys.stream()
                .filter(k -> k != null && !k.isEmpty())
                .toList();
        PaimonSplitInfo.Builder builder = PaimonSplitInfo.newBuilder()
                .setReadMode(resolveReadMode())
                .setBucketId(bucketId)
                .addAllPrimaryKeys(normalizedKeys)
                .setSnapshotId(snapshotId)
                .setFromSnapshotId(fromSnapshotId)
                .setToSnapshotId(toSnapshotId)
                .setMergeEngine(resolveMergeEngine())
                .setChangelogProducer(resolveChangelogProducer())
                .setDeletionVectorsEnabled(deletionVectorsEnabled)
                .setNeedsMerge(needsMerge)
                .setChangelogMode(changelogMode)
                .putAllPartitionValues(partitionValues)
                .putAllAggregationSpecs(aggregationSpecs);

        if (!deletionVectorFilePath.isEmpty()) {
            builder.setDeletionVectorFilePath(deletionVectorFilePath);
        }

        for (PaimonLsmLevel level : lsmLevels) {
            builder.addLsmLevels(buildLsmLevelInfo(level));
        }

        return builder.build();
    }

    private void buildSchemaEvolution(SplitInfo.Builder splitBuilder) {
        if (columnMappings.isEmpty() && typePromotions.isEmpty()) {
            return;
        }
        SchemaEvolution.Builder evoBuilder = SchemaEvolution.newBuilder();
        addColumnMappingsTo(evoBuilder);
        addTypePromotionsTo(evoBuilder);
        splitBuilder.setSchemaEvolution(evoBuilder.build());
    }

    private void buildSplitConstraint(SplitInfo.Builder splitBuilder) {
        if (partitionValues.isEmpty()) {
            return;
        }
        SplitConstraintInfo.Builder constraintBuilder = SplitConstraintInfo.newBuilder()
                .putAllPartitionValues(partitionValues);
        splitBuilder.setSplitConstraint(constraintBuilder.build());
    }

    private void addColumnMappingsTo(SchemaEvolution.Builder evoBuilder) {
        for (ColumnMapping cm : columnMappings) {
            evoBuilder.addColumnMappings(
                    org.pangrova.protocol.v1.ColumnMapping.newBuilder()
                            .setSourceFieldId(cm.sourceFieldId())
                            .setTargetFieldId(cm.targetFieldId())
                            .setSourceName(cm.sourceName())
                            .setTargetName(cm.targetName())
                            .build());
        }
    }

    private void addTypePromotionsTo(SchemaEvolution.Builder evoBuilder) {
        for (TypePromotion tp : typePromotions) {
            var builder = org.pangrova.protocol.v1.TypePromotion.newBuilder()
                    .setFieldId(tp.fieldId())
                    .setSourceType(tp.sourceType())
                    .setTargetType(tp.targetType());
            if (!tp.columnName().isEmpty()) {
                builder.setColumnName(tp.columnName());
            }
            evoBuilder.addTypePromotions(builder.build());
        }
    }

    private static LsmLevelInfo buildLsmLevelInfo(PaimonLsmLevel level) {
        return LsmLevelInfo.newBuilder()
                .setLevel(level.level())
                .addAllFilePaths(level.filePaths())
                .setTotalRecordCount(level.totalRecordCount())
                .build();
    }

    /**
     * Resolves the physical file path for the native reader.
     * Uses primaryFilePath if available, falls back to tableName for backward compatibility.
     */
    private String resolveFilePath() {
        return primaryFilePath.isEmpty() ? tableName : primaryFilePath;
    }

    /**
     * Infers the file format from the primary file path extension.
     * Paimon defaults to ORC; falls back to ORC if extension is unrecognized.
     */
    private FileFormat resolveFileFormat() {
        String path = resolveFilePath();
        if (path.endsWith(".parquet")) {
            return FileFormat.FILE_FORMAT_PARQUET;
        }
        if (path.endsWith(".lance")) {
            return FileFormat.FILE_FORMAT_LANCE;
        }
        return FileFormat.FILE_FORMAT_ORC;
    }

    private String resolveFileFormatString() {
        String path = resolveFilePath();
        if (path.endsWith(".parquet")) {
            return "parquet";
        }
        if (path.endsWith(".lance")) {
            return "lance";
        }
        return "orc";
    }

    /**
     * Estimates the byte length of this split from LSM level metadata.
     * Falls back to rowCount as an approximation when no level info is available.
     */
    private long resolveByteLength() {
        if (lsmLevels.isEmpty()) {
            return rowCount;
        }
        long total = 0;
        for (PaimonLsmLevel level : lsmLevels) {
            total += level.totalRecordCount();
        }
        return total > 0 ? total : rowCount;
    }

    private PaimonReadMode resolveReadMode() {
        if (readMode == null) {
            return PaimonReadMode.PAIMON_READ_MODE_UNSPECIFIED;
        }
        return switch (readMode.toUpperCase()) {
            case "APPEND" -> PaimonReadMode.PAIMON_READ_MODE_APPEND;
            case "PRIMARY_KEY" -> PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY;
            case "CHANGELOG" -> PaimonReadMode.PAIMON_READ_MODE_CHANGELOG;
            case "INCREMENTAL" -> PaimonReadMode.PAIMON_READ_MODE_INCREMENTAL;
            default -> PaimonReadMode.PAIMON_READ_MODE_UNSPECIFIED;
        };
    }

    private PaimonMergeEngine resolveMergeEngine() {
        if (mergeEngine == null) {
            return PaimonMergeEngine.PAIMON_MERGE_DEDUPLICATE;
        }
        return switch (mergeEngine.toUpperCase()) {
            case "PARTIAL_UPDATE" -> PaimonMergeEngine.PAIMON_MERGE_PARTIAL_UPDATE;
            case "AGGREGATION" -> PaimonMergeEngine.PAIMON_MERGE_AGGREGATION;
            case "FIRST_ROW" -> PaimonMergeEngine.PAIMON_MERGE_FIRST_ROW;
            default -> PaimonMergeEngine.PAIMON_MERGE_DEDUPLICATE;
        };
    }

    private PaimonChangelogProducer resolveChangelogProducer() {
        if (changelogProducer == null) {
            return PaimonChangelogProducer.PAIMON_CHANGELOG_NONE;
        }
        return switch (changelogProducer.toUpperCase()) {
            case "INPUT" -> PaimonChangelogProducer.PAIMON_CHANGELOG_INPUT;
            case "LOOKUP" -> PaimonChangelogProducer.PAIMON_CHANGELOG_LOOKUP;
            case "FULL_COMPACTION" -> PaimonChangelogProducer.PAIMON_CHANGELOG_FULL_COMPACTION;
            default -> PaimonChangelogProducer.PAIMON_CHANGELOG_NONE;
        };
    }

}
