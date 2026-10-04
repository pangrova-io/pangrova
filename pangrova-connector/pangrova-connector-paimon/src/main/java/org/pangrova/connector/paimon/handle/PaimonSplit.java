package org.pangrova.connector.paimon.handle;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.LsmLevelDetail;
import org.pangrova.protocol.v2.PhysicalFileReference;
import org.pangrova.protocol.v2.PhysicalFileScanDetail;
import org.pangrova.protocol.v2.PaimonChangelogProducer;
import org.pangrova.protocol.v2.PaimonMergeEngine;
import org.pangrova.protocol.v2.PaimonReadMode;
import org.pangrova.protocol.v2.PaimonSplitDetail;
import org.pangrova.protocol.v2.SchemaEvolution;
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
 * @param readMode                 canonical read mode: APPEND, PRIMARY_KEY, CHANGELOG, or INCREMENTAL
 * @param lsmLevels                LSM level descriptors for level-aware merge
 * @param needsMerge               true if MOR merge is required at read time
 * @param deletionVectorsEnabled   true if MOW deletion vectors are present
 * @param aggregationSpecs         column name → aggregation function mapping for AGGREGATION engine
 * @param changelogMode            true when reading changelog files with _row_kind column
 * @param partitionValues          partition column name-value pairs
 * @param columnMappings           schema evolution column mappings (source → target by field ID)
 * @param typePromotions           schema evolution type promotions (e.g., int → long)
 * @param primaryFile              exact physical file metadata from the owning Paimon snapshot
 * @param mergePolicy              exact table-owned retract and delete semantics
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
        Map<String, String> aggregationSpecs,
        boolean changelogMode,
        Map<String, String> partitionValues,
        List<ColumnMapping> columnMappings,
        List<TypePromotion> typePromotions,
        PaimonPhysicalFile primaryFile,
        PaimonMergePolicy mergePolicy) implements ConnectorSplit {

    public PaimonSplit {
        java.util.Objects.requireNonNull(mergePolicy, "mergePolicy");
        primaryKeys = primaryKeys == null ? List.of() : List.copyOf(primaryKeys);
        lsmLevels = lsmLevels == null ? List.of() : List.copyOf(lsmLevels);
        aggregationSpecs = aggregationSpecs == null ? Map.of() : Map.copyOf(aggregationSpecs);
        partitionValues = partitionValues == null ? Map.of() : Map.copyOf(partitionValues);
        columnMappings = columnMappings == null ? List.of() : List.copyOf(columnMappings);
        typePromotions = typePromotions == null ? List.of() : List.copyOf(typePromotions);
        java.util.Objects.requireNonNull(primaryFile, "Paimon physical file metadata");
        if (snapshotId <= 0 || rowCount < 0) {
            throw new IllegalArgumentException("Paimon physical scan requires a pinned snapshot and valid row count");
        }
        if (resolveReadMode(readMode) == PaimonReadMode.PAIMON_READ_MODE_UNSPECIFIED) {
            throw new IllegalArgumentException("Unsupported Paimon read mode: " + readMode);
        }
    }

    public String primaryFilePath() {
        return primaryFile.path();
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
     * Builds a canonical v2 DataSplit with a typed FileSplitDetail extension.
     *
     * <p>Directly constructs the SplitInfo + PaimonSplitInfo proto messages,
     * including needs_merge, deletion_vectors_enabled, aggregation_specs,
     * changelog_mode, partition_values, and schema_evolution.</p>
     *
     * <p>Validates: Requirements R14.5, R19.3</p>
     */
    @Override
    public byte[] toSplitInfoProto() {
        FileSplitDetail.Builder detail = FileSplitDetail.newBuilder()
                .setFileFormat(resolveFileFormat())
                .setPaimon(buildPaimonSplitDetail());
        var deletion = lsmLevels.stream().flatMap(level -> level.files().stream())
                .filter(file -> file.physical().equals(primaryFile)).map(PaimonLsmFile::deletionVector)
                .filter(java.util.Objects::nonNull).findFirst().orElse(null);
        detail.setPhysicalFileScan(scan(primaryFile, deletion, columnMappings, typePromotions));

        buildSchemaEvolution(detail);

        String filePath = resolveFilePath();
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "paimon", tableName, Long.toString(snapshotId), filePath, filePath,
                0L, resolveByteLength(), partitionValues, detail.build())).toByteArray();
    }

    private PaimonSplitDetail buildPaimonSplitDetail() {
        // Filter out null/empty primary keys defensively.
        List<String> normalizedKeys = primaryKeys.stream()
                .filter(k -> k != null && !k.isEmpty())
                .toList();
        PaimonSplitDetail.Builder builder = PaimonSplitDetail.newBuilder()
                .setReadMode(resolveReadMode(readMode))
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
                .setMergePolicy(org.pangrova.protocol.v2.PaimonMergePolicy.newBuilder()
                        .setIgnoreDelete(mergePolicy.ignoreDelete())
                        .setRemoveRecordOnDelete(mergePolicy.removeRecordOnDelete()))
                .putAllAggregationSpecs(aggregationSpecs);

        for (PaimonLsmLevel level : lsmLevels) {
            builder.addLsmLevels(buildLsmLevelDetail(level));
        }

        return builder.build();
    }

    private void buildSchemaEvolution(FileSplitDetail.Builder splitBuilder) {
        if (columnMappings.isEmpty() && typePromotions.isEmpty()) {
            return;
        }
        SchemaEvolution.Builder evoBuilder = SchemaEvolution.newBuilder();
        addColumnMappings(evoBuilder, columnMappings);
        addTypePromotions(evoBuilder, typePromotions);
        splitBuilder.setSchemaEvolution(evoBuilder.build());
    }

    private static void addColumnMappings(SchemaEvolution.Builder evoBuilder, List<ColumnMapping> columnMappings) {
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

    private static void addTypePromotions(SchemaEvolution.Builder evoBuilder, List<TypePromotion> typePromotions) {
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

    private static LsmLevelDetail buildLsmLevelDetail(PaimonLsmLevel level) {
        var builder = LsmLevelDetail.newBuilder()
                .setLevel(level.level())
                .setTotalRecordCount(level.totalRecordCount());
        for (var file : level.files()) {
            var physical = file.physical();
            builder.addFiles(PhysicalFileReference.newBuilder().setOpaqueLocation(physical.path())
                    .setFileFormat(format(physical.format())).setFileSize(physical.fileSize())
                    .setScan(scan(physical, file.deletionVector(), file.columnMappings(), file.typePromotions()))
                    .setMinSequenceNumber(file.minSequenceNumber())
                    .setMaxSequenceNumber(file.maxSequenceNumber()));
        }
        return builder.build();
    }

    private static PhysicalFileScanDetail scan(PaimonPhysicalFile file, PaimonDeletionVector deletion,
            List<ColumnMapping> mappings, List<TypePromotion> promotions) {
        var scan = PhysicalFileScanDetail.newBuilder().setPhysicalRows(file.rowCount())
                .setRowStart(0).setRowEnd(file.rowCount()).setFileSchemaId(file.schemaId());
        var evolution = SchemaEvolution.newBuilder();
        addColumnMappings(evolution, mappings);
        addTypePromotions(evolution, promotions);
        scan.setSchemaEvolution(evolution);
        if (deletion != null) {
            var vector = org.pangrova.protocol.v2.DeletionVectorReference.newBuilder()
                    .setOpaqueLocation(deletion.path()).setByteOffset(deletion.offset())
                    .setByteLength(deletion.length()).setPhysicalRows(deletion.physicalRows());
            if (deletion.cardinality() != null) vector.setCardinality(deletion.cardinality());
            scan.setDeletionVector(vector);
        }
        return scan.build();
    }

    /**
     * Returns the exact snapshot-owned physical path; a table name is never a file path.
     */
    private String resolveFilePath() {
        return primaryFile.path();
    }

    /**
     * Uses the SDK manifest format, including mixed-format LSM levels.
     */
    private FileFormat resolveFileFormat() {
        return format(primaryFile.format());
    }

    private static FileFormat format(String format) {
        return switch (format) {
            case "parquet" -> FileFormat.FILE_FORMAT_PARQUET;
            case "orc" -> FileFormat.FILE_FORMAT_ORC;
            case "lance" -> FileFormat.FILE_FORMAT_LANCE;
            default -> throw new IllegalStateException("Unvalidated Paimon file format");
        };
    }

    private String resolveFileFormatString() {
        return primaryFile.format();
    }

    /**
     * Physical bytes of the primary object, not a row count or sum of unrelated files.
     */
    private long resolveByteLength() {
        return primaryFile.fileSize();
    }

    private static PaimonReadMode resolveReadMode(String readMode) {
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
            return PaimonMergeEngine.PAIMON_MERGE_ENGINE_DEDUPLICATE;
        }
        return switch (mergeEngine.toUpperCase()) {
            case "PARTIAL_UPDATE" -> PaimonMergeEngine.PAIMON_MERGE_ENGINE_PARTIAL_UPDATE;
            case "AGGREGATION" -> PaimonMergeEngine.PAIMON_MERGE_ENGINE_AGGREGATION;
            case "FIRST_ROW" -> PaimonMergeEngine.PAIMON_MERGE_ENGINE_FIRST_ROW;
            default -> PaimonMergeEngine.PAIMON_MERGE_ENGINE_DEDUPLICATE;
        };
    }

    private PaimonChangelogProducer resolveChangelogProducer() {
        if (changelogProducer == null) {
            return PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_NONE;
        }
        return switch (changelogProducer.toUpperCase()) {
            case "INPUT" -> PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_INPUT;
            case "LOOKUP" -> PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_LOOKUP;
            case "FULL_COMPACTION" ->
                    PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_FULL_COMPACTION;
            default -> PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_NONE;
        };
    }

}
