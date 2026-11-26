package org.pangrova.connector.paimon.handle;

import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.PaimonChangelogProducer;
import org.pangrova.protocol.v2.PaimonMergeEngine;
import org.pangrova.protocol.v2.PaimonReadMode;
import org.pangrova.connector.api.metadata.table.ColumnMapping;
import org.pangrova.connector.api.metadata.table.TypePromotion;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract tests for Paimon's typed extension inside generic v2 DataSplit. */
class PaimonSplitProtoSerializationTest {

    @Test
    void commonEnvelopeAndTypedDetailPreserveCoreFields() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "orders", 0, 500, 3, List.of("id", "ts"),
                "DEDUPLICATE", "LOOKUP", 42, 10, 42, "PRIMARY_KEY",
                List.of(new PaimonLsmLevel(0, List.of(lsmFile("/l0/f1.parquet", 200)), 200)),
                false, false, Map.of(), false,
                Map.of("dt", "2024-01-15"), List.of(), List.of(),
                new PaimonPhysicalFile("/data/orders.parquet", "parquet", 4096, 500, 7),
                new PaimonMergePolicy(false, false));

        DataSplit data = parse(split);
        FileSplitDetail detail = DataSplitProtoCodec.fileDetail(data);
        var paimon = detail.getPaimon();

        assertThat(data.getConnectorId()).isEqualTo("paimon");
        assertThat(data.getSourceId()).isEqualTo("orders");
        assertThat(data.getSourceSnapshot()).isEqualTo("42");
        assertThat(data.getOpaqueLocation()).isEqualTo("/data/orders.parquet");
        assertThat(data.getLength()).isEqualTo(4096);
        assertThat(split.primaryFile().schemaId()).isEqualTo(7);
        assertThat(DataSplitProtoCodec.stringPartitionValues(data))
                .containsEntry("dt", "2024-01-15");
        assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
        assertThat(paimon.getReadMode()).isEqualTo(PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY);
        assertThat(paimon.getBucketId()).isEqualTo(3);
        assertThat(paimon.getPrimaryKeysList()).containsExactly("id", "ts");
        assertThat(paimon.getMergeEngine())
                .isEqualTo(PaimonMergeEngine.PAIMON_MERGE_ENGINE_DEDUPLICATE);
        assertThat(paimon.getChangelogProducer())
                .isEqualTo(PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_LOOKUP);
        assertThat(paimon.getLsmLevels(0).getFilesList()).extracting(file -> file.getOpaqueLocation())
                .containsExactly("/l0/f1.parquet");
    }

    @Test
    void mergeDeletionVectorAggregationAndChangelogArePreserved() throws Exception {
        PaimonSplit split = fullSplit("AGGREGATION", "FULL_COMPACTION", "INCREMENTAL");

        var paimon = detail(split).getPaimon();

        assertThat(paimon.getNeedsMerge()).isTrue();
        assertThat(paimon.getDeletionVectorsEnabled()).isTrue();
        var vector = paimon.getLsmLevels(0).getFiles(0).getScan().getDeletionVector();
        assertThat(vector.getOpaqueLocation()).isEqualTo("/dv/bitmap.bin");
        assertThat(vector.getByteOffset()).isEqualTo(123);
        assertThat(vector.getByteLength()).isEqualTo(48);
        assertThat(vector.getCardinality()).isEqualTo(2);
        assertThat(vector.getPhysicalRows()).isEqualTo(1000);
        assertThat(paimon.getAggregationSpecsMap()).containsEntry("amount", "SUM");
        assertThat(paimon.getChangelogMode()).isTrue();
        assertThat(paimon.getMergePolicy().getIgnoreDelete()).isFalse();
        assertThat(paimon.getMergePolicy().getRemoveRecordOnDelete()).isTrue();
        assertThat(paimon.getMergeEngine())
                .isEqualTo(PaimonMergeEngine.PAIMON_MERGE_ENGINE_AGGREGATION);
        assertThat(paimon.getChangelogProducer())
                .isEqualTo(PaimonChangelogProducer.PAIMON_CHANGELOG_PRODUCER_FULL_COMPACTION);
        assertThat(paimon.getReadMode())
                .isEqualTo(PaimonReadMode.PAIMON_READ_MODE_INCREMENTAL);
    }

    @Test
    void allEnumMappingsUseV2ConnectorContract() throws Exception {
        assertThat(detail(fullSplit("PARTIAL_UPDATE", "INPUT", "APPEND"))
                .getPaimon().getMergeEngine())
                .isEqualTo(PaimonMergeEngine.PAIMON_MERGE_ENGINE_PARTIAL_UPDATE);
        assertThat(detail(fullSplit("FIRST_ROW", "NONE", "CHANGELOG"))
                .getPaimon().getMergeEngine())
                .isEqualTo(PaimonMergeEngine.PAIMON_MERGE_ENGINE_FIRST_ROW);
        assertThat(detail(fullSplit("DEDUPLICATE", "LOOKUP", "PRIMARY_KEY"))
                .getPaimon().getReadMode())
                .isEqualTo(PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY);
    }

    @Test
    void schemaEvolutionAndFileFormatAreTyped() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "vectors", 0, 100, -1, List.of(), "DEDUPLICATE", "NONE",
                1, 0, 0, "APPEND", List.of(), false, false,
                Map.of(), false, Map.of(),
                List.of(new ColumnMapping(1, 2, "old", "new")),
                List.of(new TypePromotion(2, "float", "double", "new")),
                new PaimonPhysicalFile("/data/vectors.parquet", "parquet", 8192, 100, 0),
                new PaimonMergePolicy(false, false));

        FileSplitDetail detail = detail(split);

        assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
        assertThat(detail.getSchemaEvolution().getColumnMappings(0).getTargetName())
                .isEqualTo("new");
        assertThat(detail.getSchemaEvolution().getTypePromotions(0).getTargetType())
                .isEqualTo("double");
        assertThat(detail.getPhysicalFileScan().getSchemaEvolution()).isEqualTo(detail.getSchemaEvolution());
    }

    @Test
    void generatedV2MessageRoundTripsByteForByte() throws Exception {
        byte[] bytes = fullSplit("DEDUPLICATE", "NONE", "APPEND").toDataSplit().toByteArray();
        assertThat(DataSplit.parseFrom(bytes).toByteArray()).isEqualTo(bytes);
    }

    @Test
    void trustsSnapshotFormatNotFileNameAndNeverSubstitutesRowsForBytes() throws Exception {
        var original = fullSplit("DEDUPLICATE", "NONE", "APPEND");
        var split = new PaimonSplit(original.tableName(), original.splitRegistryId(), original.rowCount(),
                original.bucketId(), original.primaryKeys(), original.mergeEngine(), original.changelogProducer(),
                original.snapshotId(), original.fromSnapshotId(), original.toSnapshotId(), original.readMode(),
                original.lsmLevels(), original.needsMerge(), original.deletionVectorsEnabled(),
                original.aggregationSpecs(), original.changelogMode(),
                original.partitionValues(), original.columnMappings(), original.typePromotions(),
                new PaimonPhysicalFile("/data/no-extension", "ORC", 56789, 1000, 3), original.mergePolicy());
        assertThat(detail(split).getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_ORC);
        assertThat(parse(split).getLength()).isEqualTo(56789);
        assertThat(parse(split).getOpaqueLocation()).isEqualTo("/data/no-extension");
    }

    @Test
    void invalidPhysicalFileMetadataFailsInsteadOfAdvertisingOrc() {
        assertThatThrownBy(() -> new PaimonPhysicalFile("", "parquet", 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PaimonPhysicalFile("/file", "unknown", 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> new PaimonPhysicalFile("/file", "parquet", 0, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lancePhysicalFileNeverAdvertisesDatasetManifestPartition() throws Exception {
        var split = new PaimonSplit(
                "vectors", 0, 100, -1, List.of(), "DEDUPLICATE", "NONE",
                1, 0, 0, "APPEND", List.of(), false, false,
                Map.of(), false, Map.of(), List.of(), List.of(),
                new PaimonPhysicalFile("/data/vectors.lance", "lance", 4096, 100, 0),
                new PaimonMergePolicy(false, false));
        var detail = detail(split);
        assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_LANCE);
        assertThat(detail.hasScanPartition()).isFalse();
        assertThat(detail.getPhysicalFileScan().getPhysicalRows()).isEqualTo(100);
        assertThat(detail.getPhysicalFileScan().getRowEnd()).isEqualTo(100);
        assertThat(parse(split).getLength()).isEqualTo(4096);
    }

    @Test
    void rejectsReadModeThatCannotBeSerializedIntoCanonicalV2() {
        assertThatThrownBy(() -> fullSplit("DEDUPLICATE", "NONE", "FULL"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Paimon read mode");
    }

    @Test
    void allSixInputCollectionsAreOwnedAndCanonicalProtoDoesNotChange() throws Exception {
        var level = new PaimonLsmLevel(0, List.of(lsmFile("/f1.parquet", 1000)), 1000);
        var mapping = new ColumnMapping(1, 2, "old", "new");
        var promotion = new TypePromotion(2, "INTEGER", "BIGINT", "new");
        var primaryKeys = new ArrayList<>(List.of("id"));
        var levels = new ArrayList<>(List.of(level));
        var aggregations = new LinkedHashMap<>(Map.of("amount", "SUM"));
        var partitions = new LinkedHashMap<>(Map.of("dt", "2024-06-01"));
        var mappings = new ArrayList<>(List.of(mapping));
        var promotions = new ArrayList<>(List.of(promotion));
        var split = splitWithCollections(primaryKeys, levels, aggregations, partitions, mappings, promotions);
        byte[] originalBytes = CanonicalConnectorSplit.from(split).toByteArray();
        var originalDetail = DataSplitProtoCodec.fileDetail(DataSplit.parseFrom(originalBytes));
        assertThat(originalDetail.getPaimon().getPrimaryKeysList()).containsExactly("id");
        assertThat(originalDetail.getSchemaEvolution().getColumnMappings(0).getTargetName()).isEqualTo("new");
        assertThat(originalDetail.getSchemaEvolution().getTypePromotions(0).getTargetType()).isEqualTo("BIGINT");

        primaryKeys.clear();
        levels.clear();
        aggregations.clear();
        partitions.clear();
        mappings.clear();
        promotions.clear();

        assertThat(split.primaryKeys()).containsExactly("id");
        assertThat(split.lsmLevels()).containsExactly(level);
        assertThat(split.aggregationSpecs()).containsExactlyEntriesOf(Map.of("amount", "SUM"));
        assertThat(split.partitionValues()).containsExactlyEntriesOf(Map.of("dt", "2024-06-01"));
        assertThat(split.columnMappings()).containsExactly(mapping);
        assertThat(split.typePromotions()).containsExactly(promotion);
        assertThat(CanonicalConnectorSplit.from(split).toByteArray()).isEqualTo(originalBytes);
    }

    @Test
    void allSixCollectionAccessorsRejectMutationAndCanonicalProtoDoesNotChange() {
        var split = splitWithCollections(List.of("id"),
                List.of(new PaimonLsmLevel(0, List.of(lsmFile("/f1.parquet", 1000)), 1000)),
                Map.of("amount", "SUM"), Map.of("dt", "2024-06-01"),
                List.of(new ColumnMapping(1, 2, "old", "new")),
                List.of(new TypePromotion(2, "INTEGER", "BIGINT", "new")));
        byte[] originalBytes = CanonicalConnectorSplit.from(split).toByteArray();

        assertThatThrownBy(() -> split.primaryKeys().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.lsmLevels().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.aggregationSpecs().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.partitionValues().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.columnMappings().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.typePromotions().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(CanonicalConnectorSplit.from(split).toByteArray()).isEqualTo(originalBytes);
    }

    private static PaimonSplit splitWithCollections(List<String> primaryKeys, List<PaimonLsmLevel> levels,
            Map<String, String> aggregations, Map<String, String> partitions,
            List<ColumnMapping> mappings, List<TypePromotion> promotions) {
        var original = fullSplit("AGGREGATION", "NONE", "PRIMARY_KEY");
        return new PaimonSplit(original.tableName(), original.splitRegistryId(), original.rowCount(),
                original.bucketId(), primaryKeys, original.mergeEngine(), original.changelogProducer(),
                original.snapshotId(), original.fromSnapshotId(), original.toSnapshotId(), original.readMode(),
                levels, original.needsMerge(), original.deletionVectorsEnabled(), aggregations,
                original.changelogMode(), partitions, mappings, promotions,
                original.primaryFile(), original.mergePolicy());
    }

    private static PaimonSplit fullSplit(String merge, String producer, String readMode) {
        return new PaimonSplit(
                "table", 0, 1_000, 5, List.of("id"), merge, producer,
                99, 50, 99, readMode,
                List.of(new PaimonLsmLevel(0, List.of(new PaimonLsmFile(
                        new PaimonPhysicalFile("/f1.parquet", "parquet", 16384, 1000, 0), 0, 999,
                        new PaimonDeletionVector("/dv/bitmap.bin", 123, 48, 2L, 1000), List.of(), List.of())), 1000)),
                true, true, Map.of("amount", "SUM"), true,
                Map.of("dt", "2024-06-01"), List.of(), List.of(),
                new PaimonPhysicalFile("/f1.parquet", "parquet", 16384, 1000, 0),
                new PaimonMergePolicy(false, true));
    }

    private static DataSplit parse(PaimonSplit split) throws Exception {
        return DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());
    }

    private static PaimonLsmFile lsmFile(String path, long rows) {
        return new PaimonLsmFile(new PaimonPhysicalFile(path, "parquet", 4096, rows, 0),
                0, rows - 1, null, List.of(), List.of());
    }

    private static FileSplitDetail detail(PaimonSplit split) throws Exception {
        return DataSplitProtoCodec.fileDetail(parse(split));
    }
}
