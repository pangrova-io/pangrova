package org.pangrova.planner.runtimefilter;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeFilterApplicatorTest {

    private final RuntimeFilterApplicator applicator = new RuntimeFilterApplicator();

    @Test
    void minMax_prunesNonOverlappingPartitions() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 10, 20);
        List<PartitionRange> partitions = List.of(
                new PartitionRange("p1", 1, 5),
                new PartitionRange("p2", 15, 25),
                new PartitionRange("p3", 30, 40));

        List<PartitionRange> result = applicator.prunePartitions(partitions, filter);

        assertThat(result).extracting(PartitionRange::partitionId)
                .containsExactly("p2");
    }

    @Test
    void minMax_keepsOverlappingPartitions() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 10, 20);
        List<PartitionRange> partitions = List.of(
                new PartitionRange("p1", 5, 15),
                new PartitionRange("p2", 12, 18),
                new PartitionRange("p3", 15, 25));

        List<PartitionRange> result = applicator.prunePartitions(partitions, filter);

        assertThat(result).hasSize(3);
    }

    @Test
    void minMax_keepsBoundaryTouchingPartitions() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 10, 20);
        List<PartitionRange> partitions = List.of(
                new PartitionRange("p1", 5, 10),
                new PartitionRange("p2", 20, 30));

        List<PartitionRange> result = applicator.prunePartitions(partitions, filter);

        assertThat(result).hasSize(2);
    }

    @Test
    void emptyPartitionList_returnsEmpty() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 1, 100);

        List<PartitionRange> result = applicator.prunePartitions(List.of(), filter);

        assertThat(result).isEmpty();
    }

    @Test
    void bloomFilter_keepsRangePartitions() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.03)
                .put("val-A")
                .put("val-B")
                .build();
        RuntimeFilter filter = RuntimeFilter.bloom("col", sbbf);
        // Range partitions (min != max) are conservatively kept
        List<PartitionRange> partitions = List.of(
                new PartitionRange("p1", 1, 5),
                new PartitionRange("p2", 100, 200));

        List<PartitionRange> result = applicator.prunePartitions(partitions, filter);

        assertThat(result).hasSize(2);
    }

    @Test
    void bloomFilter_prunesNonMatchingSingleValuePartitions() {
        // Bloom filter contains only "region-A" and "region-B"
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.01)
                .put("region-A")
                .put("region-B")
                .build();
        RuntimeFilter filter = RuntimeFilter.bloom("region", sbbf);

        // 10 single-value partitions (min == max, representing partition column values)
        List<PartitionRange> partitions = List.of(
                new PartitionRange("p0", "region-A", "region-A"),
                new PartitionRange("p1", "region-B", "region-B"),
                new PartitionRange("p2", "region-C", "region-C"),
                new PartitionRange("p3", "region-D", "region-D"),
                new PartitionRange("p4", "region-E", "region-E"),
                new PartitionRange("p5", "region-F", "region-F"),
                new PartitionRange("p6", "region-G", "region-G"),
                new PartitionRange("p7", "region-H", "region-H"),
                new PartitionRange("p8", "region-I", "region-I"),
                new PartitionRange("p9", "region-J", "region-J"));

        RuntimeFilterApplicator freshApplicator = new RuntimeFilterApplicator();
        List<PartitionRange> result = freshApplicator.prunePartitions(partitions, filter);

        // Bloom filter matches region-A and region-B; 8 partitions pruned
        assertThat(result).extracting(PartitionRange::partitionId)
                .containsExactlyInAnyOrder("p0", "p1");
        assertThat(freshApplicator.totalEvaluated()).isEqualTo(10);
        assertThat(freshApplicator.totalPruned()).isEqualTo(8);
    }

    @Test
    void bloomFilter_tracksPruningStatistics() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.01)
                .put("val-X")
                .build();
        RuntimeFilter filter = RuntimeFilter.bloom("col", sbbf);

        List<PartitionRange> partitions = List.of(
                new PartitionRange("p1", "val-X", "val-X"),
                new PartitionRange("p2", "val-Y", "val-Y"),
                new PartitionRange("p3", "val-Z", "val-Z"));

        RuntimeFilterApplicator freshApplicator = new RuntimeFilterApplicator();
        List<PartitionRange> result = freshApplicator.prunePartitions(partitions, filter);

        assertThat(result).extracting(PartitionRange::partitionId)
                .containsExactly("p1");
        assertThat(freshApplicator.totalEvaluated()).isEqualTo(3);
        assertThat(freshApplicator.totalPruned()).isEqualTo(2);
    }

    @Test
    void bloomFilter_keepsPartitionWithNullMin() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.01)
                .put("val-A")
                .build();
        RuntimeFilter filter = RuntimeFilter.bloom("col", sbbf);

        List<PartitionRange> partitions = new ArrayList<>();
        partitions.add(new PartitionRange("p1", null, null));
        partitions.add(new PartitionRange("p2", "val-A", "val-A"));

        List<PartitionRange> result = applicator.prunePartitions(partitions, filter);

        assertThat(result).extracting(PartitionRange::partitionId)
                .containsExactlyInAnyOrder("p1", "p2");
    }

    @Test
    void pruneRowGroups_sameLogicAsPartitions() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 10, 20);
        List<PartitionRange> rowGroups = List.of(
                new PartitionRange("rg1", 1, 5),
                new PartitionRange("rg2", 15, 25));

        List<PartitionRange> result = applicator.pruneRowGroups(rowGroups, filter);

        assertThat(result).extracting(PartitionRange::partitionId)
                .containsExactly("rg2");
    }
}
