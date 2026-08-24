package org.pangrova.connector.paimon.handle;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.PaimonChangelogProducer;
import org.pangrova.protocol.v1.PaimonMergeEngine;
import org.pangrova.protocol.v1.PaimonReadMode;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PaimonSplit.toSplitInfoProto() serialization.
 * Validates: Requirements R14.5, R19.3
 */
class PaimonSplitProtoSerializationTest {

    @Test
    void serializesBasicFieldsCorrectly() throws Exception {
        PaimonSplit split = createMinimalSplit("test_table");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getConnectorId()).isEqualTo("paimon");
        assertThat(info.getFilePath()).isEqualTo("test_table");
        assertThat(info.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_ORC);
        assertThat(info.getStartOffset()).isZero();
        assertThat(info.getLength()).isEqualTo(100L);
        assertThat(info.getTableName()).isEqualTo("test_table");
    }

    @Test
    void serializesPaimonSplitInfoFields() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "orders", 0L, 500L,
                3, List.of("id", "ts"), "DEDUPLICATE", "LOOKUP",
                42L, 10L, 42L, "PRIMARY_KEY",
                List.of(new PaimonLsmLevel(0, List.of("/l0/f1.parquet"), 200L)),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasPaimonInfo()).isTrue();
        var paimon = info.getPaimonInfo();
        assertThat(paimon.getReadMode()).isEqualTo(PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY);
        assertThat(paimon.getBucketId()).isEqualTo(3);
        assertThat(paimon.getPrimaryKeysList()).containsExactly("id", "ts");
        assertThat(paimon.getSnapshotId()).isEqualTo(42L);
        assertThat(paimon.getFromSnapshotId()).isEqualTo(10L);
        assertThat(paimon.getToSnapshotId()).isEqualTo(42L);
        assertThat(paimon.getMergeEngine()).isEqualTo(PaimonMergeEngine.PAIMON_MERGE_DEDUPLICATE);
        assertThat(paimon.getChangelogProducer()).isEqualTo(PaimonChangelogProducer.PAIMON_CHANGELOG_LOOKUP);
    }

    @Test
    void serializesNeedsMergeFlag() throws Exception {
        PaimonSplit split = createFullSplit(true, false, "", Map.of(), false);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getNeedsMerge()).isTrue();
    }

    @Test
    void serializesDeletionVectorFields() throws Exception {
        PaimonSplit split = createFullSplit(
                false, true, "/dv/bitmap-001.bin", Map.of(), false);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        var paimon = info.getPaimonInfo();
        assertThat(paimon.getDeletionVectorsEnabled()).isTrue();
        assertThat(paimon.getDeletionVectorFilePath()).isEqualTo("/dv/bitmap-001.bin");
        assertThat(paimon.getNeedsMerge()).isFalse();
    }

    @Test
    void serializesAggregationSpecs() throws Exception {
        Map<String, String> aggSpecs = Map.of("amount", "SUM", "count", "COUNT");
        PaimonSplit split = createFullSplit(true, false, "", aggSpecs, false);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getAggregationSpecsMap())
                .containsEntry("amount", "SUM")
                .containsEntry("count", "COUNT");
    }

    @Test
    void serializesChangelogMode() throws Exception {
        PaimonSplit split = createFullSplit(false, false, "", Map.of(), true);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getChangelogMode()).isTrue();
    }

    @Test
    void serializesPartitionValues() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "partitioned_table", 0L, 100L,
                0, List.of(), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false,
                Map.of("dt", "2024-01-15", "region", "us-east"),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getPartitionValuesMap())
                .containsEntry("dt", "2024-01-15")
                .containsEntry("region", "us-east");
    }

    @Test
    void serializesSchemaEvolution() throws Exception {
        List<ColumnMapping> mappings = List.of(
                new ColumnMapping(1, 1, "old_id", "id"),
                new ColumnMapping(2, 3, "old_name", "full_name"));
        List<TypePromotion> promotions = List.of(
                new TypePromotion(1, "int", "long"));

        PaimonSplit split = new PaimonSplit(
                "evolved_table", 0L, 100L,
                -1, List.of(), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false, Map.of(),
                mappings, promotions, "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasSchemaEvolution()).isTrue();
        assertThat(info.getSchemaEvolution().getColumnMappingsCount()).isEqualTo(2);
        assertThat(info.getSchemaEvolution().getColumnMappings(0).getSourceName()).isEqualTo("old_id");
        assertThat(info.getSchemaEvolution().getColumnMappings(0).getTargetName()).isEqualTo("id");
        assertThat(info.getSchemaEvolution().getColumnMappings(1).getSourceFieldId()).isEqualTo(2);
        assertThat(info.getSchemaEvolution().getColumnMappings(1).getTargetFieldId()).isEqualTo(3);

        assertThat(info.getSchemaEvolution().getTypePromotionsCount()).isEqualTo(1);
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getFieldId()).isEqualTo(1);
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getSourceType()).isEqualTo("int");
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getTargetType()).isEqualTo("long");
    }

    @Test
    void omitsSchemaEvolutionWhenEmpty() throws Exception {
        PaimonSplit split = createMinimalSplit("table");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasSchemaEvolution()).isFalse();
    }

    @Test
    void serializesLsmLevels() throws Exception {
        List<PaimonLsmLevel> levels = List.of(
                new PaimonLsmLevel(0, List.of("/l0/a.parquet", "/l0/b.parquet"), 300L),
                new PaimonLsmLevel(1, List.of("/l1/c.parquet"), 500L));

        PaimonSplit split = new PaimonSplit(
                "pk_table", 0L, 800L,
                0, List.of("pk"), "DEDUPLICATE", "NONE",
                5L, 0L, 0L, "PRIMARY_KEY", levels,
                true, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getLsmLevelsCount()).isEqualTo(2);
        var l0 = info.getPaimonInfo().getLsmLevels(0);
        assertThat(l0.getLevel()).isZero();
        assertThat(l0.getFilePathsList()).containsExactly("/l0/a.parquet", "/l0/b.parquet");
        assertThat(l0.getTotalRecordCount()).isEqualTo(300L);
        var l1 = info.getPaimonInfo().getLsmLevels(1);
        assertThat(l1.getLevel()).isEqualTo(1);
        assertThat(l1.getFilePathsList()).containsExactly("/l1/c.parquet");
        assertThat(l1.getTotalRecordCount()).isEqualTo(500L);
    }

    @Test
    void serializesMergeEngineVariants() throws Exception {
        assertMergeEngine("PARTIAL_UPDATE", PaimonMergeEngine.PAIMON_MERGE_PARTIAL_UPDATE);
        assertMergeEngine("AGGREGATION", PaimonMergeEngine.PAIMON_MERGE_AGGREGATION);
        assertMergeEngine("FIRST_ROW", PaimonMergeEngine.PAIMON_MERGE_FIRST_ROW);
        assertMergeEngine("DEDUPLICATE", PaimonMergeEngine.PAIMON_MERGE_DEDUPLICATE);
    }

    @Test
    void serializesReadModeVariants() throws Exception {
        assertReadMode("APPEND", PaimonReadMode.PAIMON_READ_MODE_APPEND);
        assertReadMode("PRIMARY_KEY", PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY);
        assertReadMode("CHANGELOG", PaimonReadMode.PAIMON_READ_MODE_CHANGELOG);
        assertReadMode("INCREMENTAL", PaimonReadMode.PAIMON_READ_MODE_INCREMENTAL);
    }

    @Test
    void serializesChangelogProducerVariants() throws Exception {
        assertChangelogProducer("INPUT", PaimonChangelogProducer.PAIMON_CHANGELOG_INPUT);
        assertChangelogProducer("LOOKUP", PaimonChangelogProducer.PAIMON_CHANGELOG_LOOKUP);
        assertChangelogProducer("FULL_COMPACTION", PaimonChangelogProducer.PAIMON_CHANGELOG_FULL_COMPACTION);
        assertChangelogProducer("NONE", PaimonChangelogProducer.PAIMON_CHANGELOG_NONE);
    }

    @Test
    void protoRoundTripPreservesAllFields() throws Exception {
        List<ColumnMapping> mappings = List.of(new ColumnMapping(1, 2, "src", "tgt"));
        List<TypePromotion> promotions = List.of(new TypePromotion(1, "int", "long"));

        PaimonSplit split = new PaimonSplit(
                "full_table", 0L, 1000L,
                5, List.of("id", "ts"), "AGGREGATION", "FULL_COMPACTION",
                99L, 50L, 99L, "PRIMARY_KEY",
                List.of(new PaimonLsmLevel(0, List.of("/f1.parquet"), 400L)),
                true, true, "/dv/bm.bin",
                Map.of("val", "SUM"), true,
                Map.of("dt", "2024-06-01"),
                mappings, promotions, "");

        byte[] bytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(bytes);

        // Re-serialize and verify byte-level equality
        byte[] roundTripped = info.toByteArray();
        assertThat(roundTripped).isEqualTo(bytes);
    }

    @Test
    void omitsDeletionVectorFilePathWhenEmpty() throws Exception {
        PaimonSplit split = createFullSplit(false, true, "", Map.of(), false);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getDeletionVectorsEnabled()).isTrue();
        assertThat(info.getPaimonInfo().getDeletionVectorFilePath()).isEmpty();
    }

    @Test
    void backwardCompatibleConstructorProducesValidProto() throws Exception {
        PaimonSplit split = new PaimonSplit("legacy_table", 0L, 50L,
                -1, List.of(), "DEDUPLICATE", "NONE",
                0L, 0L, 0L, "FULL", List.of(),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasPaimonInfo()).isTrue();
        var paimon = info.getPaimonInfo();
        assertThat(paimon.getNeedsMerge()).isFalse();
        assertThat(paimon.getDeletionVectorsEnabled()).isFalse();
        assertThat(paimon.getChangelogMode()).isFalse();
        assertThat(paimon.getAggregationSpecsMap()).isEmpty();
        assertThat(paimon.getPartitionValuesMap()).isEmpty();
    }

    // -- helpers --

    private static PaimonSplit createMinimalSplit(String tableName) {
        return new PaimonSplit(tableName, 0L, 100L,
                -1, List.of(), "DEDUPLICATE", "NONE",
                0L, 0L, 0L, "FULL", List.of(),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");
    }

    private static PaimonSplit createFullSplit(
            boolean needsMerge,
            boolean dvEnabled,
            String dvFilePath,
            Map<String, String> aggSpecs,
            boolean changelogMode) {
        return new PaimonSplit(
                "test_table", 0L, 100L,
                0, List.of("pk"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "PRIMARY_KEY", List.of(),
                needsMerge, dvEnabled, dvFilePath, aggSpecs, changelogMode,
                Map.of(), List.of(), List.of(), "");
    }

    private void assertMergeEngine(String engineName, PaimonMergeEngine expected) throws Exception {
        PaimonSplit split = new PaimonSplit(
                "t", 0L, 10L,
                -1, List.of(), engineName, "NONE",
                0L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");
        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());
        assertThat(info.getPaimonInfo().getMergeEngine()).isEqualTo(expected);
    }

    private void assertReadMode(String modeName, PaimonReadMode expected) throws Exception {
        PaimonSplit split = new PaimonSplit(
                "t", 0L, 10L,
                -1, List.of(), "DEDUPLICATE", "NONE",
                0L, 0L, 0L, modeName, List.of(),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");
        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());
        assertThat(info.getPaimonInfo().getReadMode()).isEqualTo(expected);
    }

    private void assertChangelogProducer(String producerName, PaimonChangelogProducer expected) throws Exception {
        PaimonSplit split = new PaimonSplit(
                "t", 0L, 10L,
                -1, List.of(), "DEDUPLICATE", producerName,
                0L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false, Map.of(),
                List.of(), List.of(), "");
        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());
        assertThat(info.getPaimonInfo().getChangelogProducer()).isEqualTo(expected);
    }
}
