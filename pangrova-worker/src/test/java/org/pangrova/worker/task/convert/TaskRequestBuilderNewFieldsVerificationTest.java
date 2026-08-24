package org.pangrova.worker.task.convert;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Base64;
import java.util.List;

import org.pangrova.protocol.v1.ColumnMapping;
import org.pangrova.protocol.v1.PaimonSplitInfo;
import org.pangrova.protocol.v1.SchemaEvolution;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.v1.TypePromotion;
import org.junit.jupiter.api.Test;

/**
 * Verifies that TaskRequestBuilder.decodeSplitInfos() correctly parses
 * SplitInfo proto messages containing new fields added for the native
 * connector Velox consolidation:
 * - partition_values
 * - schema_evolution (column_mappings + type_promotions)
 * - paimon_info (needs_merge, deletion_vectors_enabled, etc.)
 *
 * Validates: Requirements R20.1
 */
class TaskRequestBuilderNewFieldsVerificationTest {

    @Test
    void decodeSplitInfoPreservesPartitionValues() {
        SplitInfo proto = SplitInfo.newBuilder()
                .setSplitId("split-1")
                .setConnectorId("iceberg")
                .putPartitionValues("year", "2025")
                .putPartitionValues("month", "03")
                .build();

        String encoded = Base64.getEncoder().encodeToString(proto.toByteArray());
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(List.of(encoded));

        assertEquals(1, decoded.size());
        SplitInfo result = decoded.get(0);
        assertEquals("2025", result.getPartitionValuesMap().get("year"));
        assertEquals("03", result.getPartitionValuesMap().get("month"));
    }

    @Test
    void decodeSplitInfoPreservesSchemaEvolution() {
        ColumnMapping mapping = ColumnMapping.newBuilder()
                .setSourceFieldId(1)
                .setTargetFieldId(2)
                .setSourceName("old_col")
                .setTargetName("new_col")
                .build();
        TypePromotion promotion = TypePromotion.newBuilder()
                .setFieldId(3)
                .setSourceType("INT")
                .setTargetType("BIGINT")
                .build();
        SchemaEvolution evolution = SchemaEvolution.newBuilder()
                .addColumnMappings(mapping)
                .addTypePromotions(promotion)
                .build();

        SplitInfo proto = SplitInfo.newBuilder()
                .setSplitId("split-2")
                .setSchemaEvolution(evolution)
                .build();

        String encoded = Base64.getEncoder().encodeToString(proto.toByteArray());
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(List.of(encoded));

        assertEquals(1, decoded.size());
        SchemaEvolution result = decoded.get(0).getSchemaEvolution();
        assertEquals(1, result.getColumnMappingsCount());
        assertEquals("old_col", result.getColumnMappings(0).getSourceName());
        assertEquals(1, result.getTypePromotionsCount());
        assertEquals("BIGINT", result.getTypePromotions(0).getTargetType());
    }

    @Test
    void decodeSplitInfoPreservesPaimonMorFields() {
        PaimonSplitInfo paimon = PaimonSplitInfo.newBuilder()
                .setNeedsMerge(true)
                .setDeletionVectorsEnabled(false)
                .setChangelogMode(false)
                .setSequenceColumn("_seq")
                .putAggregationSpecs("amount", "SUM")
                .putAggregationSpecs("count", "COUNT")
                .putPartitionValues("dt", "2025-03-07")
                .build();

        SplitInfo proto = SplitInfo.newBuilder()
                .setSplitId("split-3")
                .setConnectorId("paimon")
                .setPaimonInfo(paimon)
                .build();

        String encoded = Base64.getEncoder().encodeToString(proto.toByteArray());
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(List.of(encoded));

        assertEquals(1, decoded.size());
        PaimonSplitInfo result = decoded.get(0).getPaimonInfo();
        assertTrue(result.getNeedsMerge());
        assertFalse(result.getDeletionVectorsEnabled());
        assertEquals("_seq", result.getSequenceColumn());
        assertEquals("SUM", result.getAggregationSpecsMap().get("amount"));
        assertEquals("COUNT", result.getAggregationSpecsMap().get("count"));
        assertEquals("2025-03-07", result.getPartitionValuesMap().get("dt"));
    }

    @Test
    void decodeSplitInfoPreservesPaimonMowDvFields() {
        PaimonSplitInfo paimon = PaimonSplitInfo.newBuilder()
                .setNeedsMerge(false)
                .setDeletionVectorsEnabled(true)
                .setDeletionVectorFilePath("/data/dv/bucket-0.dv")
                .build();

        SplitInfo proto = SplitInfo.newBuilder()
                .setSplitId("split-4")
                .setConnectorId("paimon")
                .setPaimonInfo(paimon)
                .build();

        String encoded = Base64.getEncoder().encodeToString(proto.toByteArray());
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(List.of(encoded));

        PaimonSplitInfo result = decoded.get(0).getPaimonInfo();
        assertFalse(result.getNeedsMerge());
        assertTrue(result.getDeletionVectorsEnabled());
        assertEquals("/data/dv/bucket-0.dv", result.getDeletionVectorFilePath());
    }

    @Test
    void decodeSplitInfoHandlesEmptyList() {
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(List.of());
        assertTrue(decoded.isEmpty());
    }

    @Test
    void decodeSplitInfoHandlesNull() {
        List<SplitInfo> decoded = TaskRequestBuilder.decodeSplitInfos(null);
        assertTrue(decoded.isEmpty());
    }

    @Test
    void decodeSplitInfoRejectsInvalidBase64() {
        assertThrows(IllegalArgumentException.class, () ->
                TaskRequestBuilder.decodeSplitInfos(List.of("not-valid-base64!!!")));
    }
}
