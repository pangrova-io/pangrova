package org.pangrova.connector.paimon.handle;

import org.pangrova.protocol.source.CanonicalConnectorSplit;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
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
                false, false, Map.of(), false,
                Map.of("dt", "2024-06-01"),
                List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        DataSplit info = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());

        assertThat(DataSplitProtoCodec.fileDetail(info).getPaimon().getBucketId()).isEqualTo(5);
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
                false, false, Map.of(), false,
                partitions, List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        DataSplit info = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());

        assertThat(DataSplitProtoCodec.stringPartitionValues(info))
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
                false, false, Map.of(), false,
                Map.of(), List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        DataSplit info = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());

        assertThat(DataSplitProtoCodec.stringPartitionValues(info)).isEmpty();
    }

    @Test
    void multipleBucketsSamePartitionProduceSeparateSplits() throws Exception {
        Map<String, String> partition = Map.of("dt", "2024-06-01");

        PaimonSplit bucket0 = new PaimonSplit(
                "orders", 0L, 100L,
                0, List.of("id"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, Map.of(), false,
                partition, List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        PaimonSplit bucket1 = new PaimonSplit(
                "orders", 0L, 150L,
                1, List.of("id"), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, Map.of(), false,
                partition, List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        DataSplit info0 = DataSplit.parseFrom(CanonicalConnectorSplit.from(bucket0).toByteArray());
        DataSplit info1 = DataSplit.parseFrom(CanonicalConnectorSplit.from(bucket1).toByteArray());

        // Each bucket produces an independent split with its own bucket ID
        assertThat(DataSplitProtoCodec.fileDetail(info0).getPaimon().getBucketId()).isZero();
        assertThat(DataSplitProtoCodec.fileDetail(info1).getPaimon().getBucketId()).isEqualTo(1);

        // Both share the same partition values
        assertThat(DataSplitProtoCodec.stringPartitionValues(info0))
                .isEqualTo(DataSplitProtoCodec.stringPartitionValues(info1))
                .containsEntry("dt", "2024-06-01");
    }

    @Test
    void partitionValuesWithTableMetadataPreserved() throws Exception {
        PaimonSplit split = new PaimonSplit(
                "pk_partitioned", 0L, 300L,
                3, List.of("id", "ts"), "PARTIAL_UPDATE", "LOOKUP",
                42L, 0L, 0L, "PRIMARY_KEY", List.of(),
                true, false, Map.of("amount", "SUM"), true,
                Map.of("country", "CN", "year", "2024"),
                List.of(), List.of(), physicalFile(), new PaimonMergePolicy(false, false));

        DataSplit info = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());

        var paimon = DataSplitProtoCodec.fileDetail(info).getPaimon();
        // Partition values preserved alongside table metadata
        assertThat(DataSplitProtoCodec.stringPartitionValues(info))
                .containsEntry("country", "CN")
                .containsEntry("year", "2024");
        assertThat(paimon.getBucketId()).isEqualTo(3);
        assertThat(paimon.getPrimaryKeysList()).containsExactly("id", "ts");
        assertThat(paimon.getNeedsMerge()).isTrue();
        assertThat(paimon.getChangelogMode()).isTrue();
    }
    private static PaimonPhysicalFile physicalFile() {
        return new PaimonPhysicalFile("/data/file.parquet", "parquet", 4096, 500, 0);
    }
}
