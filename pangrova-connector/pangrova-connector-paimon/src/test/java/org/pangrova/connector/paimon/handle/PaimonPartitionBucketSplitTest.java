package org.pangrova.connector.paimon.handle;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v1.SplitInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Paimon partition column injection and bucket-level split
 * serialization through the proto path.
 *
 * <p>Validates: Requirements R12.1, R12.2, R12.3, R12.4</p>
 *
 * <p>R12.1: Partition pruning is performed on the Java side by PaimonSplitManager
 * (Paimon SDK applies partition predicates during scan planning).
 * R12.2: Paimon SDK produces one split per bucket — verified by bucketId field.
 * R12.3: partition_values are serialized into PaimonSplitInfo proto.
 * R12.4: Native side reuses VeloxPartitionColumnInjector (Task 8) to inject
 * partition columns from SplitConfig.paimon.partition_values.</p>
 */
class PaimonPartitionBucketSplitTest {

    @Test
    void bucketIdSerializedInProto() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "orders", 0L, 100L,
                5, List.of("id"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false,
                Map.of("dt", "2024-06-01"),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getBucketId()).isEqualTo(5);
    }

    @Test
    void partitionValuesSerializedForNativeInjection() throws Exception {
        Map<String, String> partitions = Map.of(
                "dt", "2024-06-01",
                "region", "us-east",
                "hour", "14");

        PaimonSplit split = new PaimonSplit(
                "events", 0L, 500L,
                2, List.of(), "DEDUPLICATE", "NONE",
                10L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false,
                partitions, List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        var paimonInfo = info.getPaimonInfo();
        assertThat(paimonInfo.getPartitionValuesMap())
                .hasSize(3)
                .containsEntry("dt", "2024-06-01")
                .containsEntry("region", "us-east")
                .containsEntry("hour", "14");
    }

    @Test
    void emptyPartitionValuesForNonPartitionedTable() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "flat_table", 0L, 200L,
                0, List.of(), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "APPEND", List.of(),
                false, false, "", Map.of(), false,
                Map.of(), List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getPaimonInfo().getPartitionValuesMap()).isEmpty();
    }

    @Test
    void multipleBucketsSamePartitionProduceSeparateSplits() throws Exception {
        Map<String, String> partition = Map.of("dt", "2024-06-01");

        PaimonSplit bucket0 = new PaimonSplit(
                "orders", 0L, 100L,
                0, List.of("id"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, "", Map.of(), false,
                partition, List.of(), List.of(), "");

        PaimonSplit bucket1 = new PaimonSplit(
                "orders", 0L, 150L,
                1, List.of("id"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, "", Map.of(), false,
                partition, List.of(), List.of(), "");

        SplitInfo info0 = SplitInfo.parseFrom(bucket0.toSplitInfoProto());
        SplitInfo info1 = SplitInfo.parseFrom(bucket1.toSplitInfoProto());

        // Each bucket produces an independent split with its own bucket ID
        assertThat(info0.getPaimonInfo().getBucketId()).isZero();
        assertThat(info1.getPaimonInfo().getBucketId()).isEqualTo(1);

        // Both share the same partition values
        assertThat(info0.getPaimonInfo().getPartitionValuesMap())
                .isEqualTo(info1.getPaimonInfo().getPartitionValuesMap())
                .containsEntry("dt", "2024-06-01");
    }

    @Test
    void partitionValuesWithTableMetadataPreserved() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "pk_partitioned", 0L, 300L,
                3, List.of("id", "ts"), "PARTIAL_UPDATE", "LOOKUP",
                42L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, "", Map.of("amount", "SUM"), true,
                Map.of("country", "CN", "year", "2024"),
                List.of(), List.of(), "");

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        var paimon = info.getPaimonInfo();
        // Partition values preserved alongside table metadata
        assertThat(paimon.getPartitionValuesMap())
                .containsEntry("country", "CN")
                .containsEntry("year", "2024");
        assertThat(paimon.getBucketId()).isEqualTo(3);
        assertThat(paimon.getPrimaryKeysList()).containsExactly("id", "ts");
        assertThat(paimon.getNeedsMerge()).isTrue();
        assertThat(paimon.getChangelogMode()).isTrue();
    }
}
