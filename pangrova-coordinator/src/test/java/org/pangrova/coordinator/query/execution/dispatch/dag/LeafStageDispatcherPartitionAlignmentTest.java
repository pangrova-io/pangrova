package org.pangrova.coordinator.query.execution.dispatch.dag;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LeafStageDispatcher#buildPartitionFilePaths}, verifying
 * that file paths are partitioned across task partitions using round-robin
 * distribution (the current implementation).
 */
class LeafStageDispatcherPartitionAlignmentTest {

    @Test
    void uniformSplits_twoPartitions() {
        List<SplitFetcher.SplitWithPaths> splits = List.of(
                new SplitFetcher.SplitWithPaths("aaaa", List.of("s3://bucket/file0.parquet")),
                new SplitFetcher.SplitWithPaths("bbbb", List.of("s3://bucket/file1.parquet")),
                new SplitFetcher.SplitWithPaths("cccc", List.of("s3://bucket/file2.parquet")),
                new SplitFetcher.SplitWithPaths("dddd", List.of("s3://bucket/file3.parquet")));

        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(splits, 2);

        assertThat(paths).hasSize(2);
        // Round-robin: split 0,2 -> partition 0; split 1,3 -> partition 1
        assertThat(paths.get(0)).containsExactly(
                "s3://bucket/file0.parquet", "s3://bucket/file2.parquet");
        assertThat(paths.get(1)).containsExactly(
                "s3://bucket/file1.parquet", "s3://bucket/file3.parquet");
    }

    @Test
    void singlePartition_getsAllPaths() {
        List<SplitFetcher.SplitWithPaths> splits = List.of(
                new SplitFetcher.SplitWithPaths("aaa", List.of("s3://f1")),
                new SplitFetcher.SplitWithPaths("bb", List.of("s3://f2")),
                new SplitFetcher.SplitWithPaths("c", List.of("s3://f3")));

        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(splits, 1);

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0)).containsExactly("s3://f1", "s3://f2", "s3://f3");
    }

    @Test
    void emptyInput_producesEmptyPartitions() {
        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(List.of(), 3);

        assertThat(paths).hasSize(3);
        for (int p = 0; p < 3; p++) {
            assertThat(paths.get(p)).isEmpty();
        }
    }

    @Test
    void multiFileSplits_allPathsDistributed() {
        List<SplitFetcher.SplitWithPaths> splits = List.of(
                new SplitFetcher.SplitWithPaths("aaa",
                        List.of("s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet")),
                new SplitFetcher.SplitWithPaths("bbbbbbbbb",
                        List.of("s3://bucket/data/f3.parquet")));

        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(splits, 2);

        assertThat(paths).hasSize(2);
        // Split 0 -> partition 0 (all its file paths)
        assertThat(paths.get(0)).containsExactly(
                "s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet");
        // Split 1 -> partition 1
        assertThat(paths.get(1)).containsExactly("s3://bucket/data/f3.parquet");
    }

    @Test
    void splitsWithEmptyFilePaths() {
        List<SplitFetcher.SplitWithPaths> splits = List.of(
                new SplitFetcher.SplitWithPaths("aaaa", List.of("s3://f1")),
                new SplitFetcher.SplitWithPaths("bbb", List.of()),
                new SplitFetcher.SplitWithPaths("cc", List.of("s3://f3")));

        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(splits, 2);

        // Verify no phantom paths appear
        int totalPaths = paths.stream().mapToInt(List::size).sum();
        assertThat(totalPaths).isEqualTo(2); // only f1 and f3
    }

    @Test
    void morePartitionsThanSplits_extraPartitionsEmpty() {
        List<SplitFetcher.SplitWithPaths> splits = List.of(
                new SplitFetcher.SplitWithPaths("aaa", List.of("s3://f1")));

        List<List<String>> paths = LeafStageDispatcher.buildPartitionFilePaths(splits, 4);

        assertThat(paths).hasSize(4);
        assertThat(paths.get(0)).containsExactly("s3://f1");
        assertThat(paths.get(1)).isEmpty();
        assertThat(paths.get(2)).isEmpty();
        assertThat(paths.get(3)).isEmpty();
    }
}
