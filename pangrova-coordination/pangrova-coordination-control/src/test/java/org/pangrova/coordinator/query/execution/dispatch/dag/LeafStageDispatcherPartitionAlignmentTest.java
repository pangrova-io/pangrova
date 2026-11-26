package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.protocol.v2.DataSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.protocol.v2.BatchSerde;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LeafStageDispatcher#buildPartitionFilePaths}, verifying
 * that cache-affinity paths follow the exact split partition assignment.
 */
class LeafStageDispatcherPartitionAlignmentTest {

    @Test
    void everyMaterializedDistributionStartsTheNativeProducer() {
        for (ShuffleType distribution : List.of(
                ShuffleType.HASH, ShuffleType.BROADCAST,
                ShuffleType.ROUND_ROBIN, ShuffleType.SINGLETON)) {
            int partitions = distribution == ShuffleType.SINGLETON ? 1 : 2;
            StageNode producer = stageNode(1, 1);
            StageNode consumer = stageNode(0, partitions);
            StageEdge edge = new StageEdge(1, 0, distribution,
                    distribution == ShuffleType.HASH ? List.of(0) : List.of(),
                    "exchange-1-to-0", partitions,
                    BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
            StageDAG dag = StageDAG.fromNodes(
                    List.of(consumer, producer), Map.of(0, List.of(1)), List.of(edge));

            var config = LeafStageDispatcher.buildShuffleConfig(producer, dag);
            assertThat(config).as("%s producer must advertise Flight endpoint", distribution)
                    .isNotNull();
            assertThat(config.numPartitions()).isEqualTo(partitions);
            assertThat(config.serdeFormat()).isEqualTo(edge.batchSerde().name().substring("BATCH_SERDE_".length()));
            assertThat(config.compressionCodec()).isEqualTo("lz4");
            assertThat(config.partitionKeyIndices())
                    .containsExactlyElementsOf(edge.distributionKeys());
        }
    }

    @Test
    void externalShuffleRejectsBroadcastWithoutARegisteredExternalOutput() {
        StageNode producer = stageNode(1, 1);
        StageNode consumer = stageNode(0, 2);
        StageEdge edge = new StageEdge(1, 0, ShuffleType.BROADCAST,
                List.of(), "exchange-1-to-0", 2,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
        StageDAG dag = StageDAG.fromNodes(
                List.of(consumer, producer), Map.of(0, List.of(1)), List.of(edge));

        assertThatThrownBy(() -> LeafStageDispatcher.buildShuffleConfig(
                producer, dag, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("External shuffle does not support");
    }

    @Test
    void externalShuffleMaterializesSingletonAndRoundRobinWithoutDirectFallback() {
        for (var distribution : List.of(ShuffleType.SINGLETON, ShuffleType.ROUND_ROBIN)) {
            int partitions = distribution == ShuffleType.SINGLETON ? 1 : 4;
            var producer = stageNode(1, 4);
            var consumer = stageNode(0, partitions);
            var edge = new StageEdge(1, 0, distribution, List.of(), "remote", partitions,
                    BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
            var graph = StageDAG.fromNodes(List.of(consumer, producer), Map.of(0, List.of(1)), List.of(edge));
            var config = LeafStageDispatcher.buildShuffleConfig(producer, graph, true);
            assertThat(config.persistenceMode()).isEqualTo("MEMORY_AND_REMOTE");
            assertThat(config.serdeFormat()).isEqualTo("ARROW_BATCH");
            assertThat(config.numPartitions()).isEqualTo(partitions);
        }
    }

    @Test
    void exchangeConsumerAndRegisteredProducerCountsCannotBeCapacityTruncated() {
        var producer = stageNode(1, 8);
        var consumer = stageNode(0, 8);
        var edge = new StageEdge(1, 0, ShuffleType.HASH, List.of(0), "complete", 8,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
        var graph = StageDAG.fromNodes(List.of(consumer, producer), Map.of(0, List.of(1)), List.of(edge));
        assertThat(LeafStageDispatcher.taskCountForGraph(consumer, graph, false, 8, 4)).isEqualTo(8);
        assertThat(LeafStageDispatcher.taskCountForGraph(producer, graph, true, 2, 4)).isEqualTo(8);
        assertThat(LeafStageDispatcher.taskCountForGraph(producer, graph, false, 2, 4)).isEqualTo(2);
    }

    private static StageNode stageNode(int stageId, int parallelism) {
        var cluster = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        return new StageNode(stageId,
                new PlanFragment(LogicalValues.createOneRow(cluster), -1, -1),
                parallelism, "COMPUTE", List.of());
    }

    @Test
    void uniformSplits_twoPartitions() {
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("aaaa"), List.of("s3://bucket/file0.parquet")),
                new QuerySplit(split("bbbb"), List.of("s3://bucket/file1.parquet")),
                new QuerySplit(split("cccc"), List.of("s3://bucket/file2.parquet")),
                new QuerySplit(split("dddd"), List.of("s3://bucket/file3.parquet")));

        List<List<String>> paths = pathsFor(splits, 2);

        assertThat(paths).hasSize(2);
        // Equal-size splits retain input order under LPT assignment.
        assertThat(paths.get(0)).containsExactly(
                "s3://bucket/file0.parquet", "s3://bucket/file2.parquet");
        assertThat(paths.get(1)).containsExactly(
                "s3://bucket/file1.parquet", "s3://bucket/file3.parquet");
    }

    @Test
    void singlePartition_getsAllPaths() {
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("aaa"), List.of("s3://f1")),
                new QuerySplit(split("bb"), List.of("s3://f2")),
                new QuerySplit(split("c"), List.of("s3://f3")));

        List<List<String>> paths = pathsFor(splits, 1);

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0)).containsExactly("s3://f1", "s3://f2", "s3://f3");
    }

    @Test
    void emptyInput_producesEmptyPartitions() {
        List<List<String>> paths = pathsFor(List.of(), 3);

        assertThat(paths).hasSize(3);
        for (int p = 0; p < 3; p++) {
            assertThat(paths.get(p)).isEmpty();
        }
    }

    @Test
    void multiFileSplits_allPathsDistributed() {
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("serialized-small-scan"),
                        List.of("s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet"), 512),
                new QuerySplit(split("big"),
                        List.of("s3://bucket/data/f3.parquet"), 4096));

        List<List<String>> paths = pathsFor(splits, 2);

        assertThat(paths).hasSize(2);
        // Physical scan size determines placement.
        assertThat(paths.get(0)).containsExactly(
                "s3://bucket/data/f3.parquet");
        assertThat(paths.get(1)).containsExactly(
                "s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet");
    }

    @Test
    void equalLengthDescriptorsWithUnequalScanRangesBalanceTheActualBytes() {
        long large = 512L * 1024 * 1024;
        long small = 512L * 1024;
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("scan-a"), List.of("s3://a"), large),
                new QuerySplit(split("scan-b"), List.of("s3://b"), small),
                new QuerySplit(split("scan-c"), List.of("s3://c"), large),
                new QuerySplit(split("scan-d"), List.of("s3://d"), small));

        assertThat(pathsFor(splits, 2)).containsExactly(
                List.of("s3://a", "s3://b"), List.of("s3://c", "s3://d"));
    }

    @Test
    void largeCohortsAssignEverySplitInDeterministicPartitionOrder() {
        var splits = java.util.stream.IntStream.range(0, 256)
                .mapToObj(index -> new QuerySplit(split("split-" + index), List.of("s3://" + index), 1))
                .toList();

        var paths = pathsFor(splits, 128);

        assertThat(paths).hasSize(128);
        for (int partition = 0; partition < paths.size(); partition++) {
            assertThat(paths.get(partition)).containsExactly(
                    "s3://" + partition, "s3://" + (partition + 128));
        }
    }

    @Test
    void splitsWithEmptyFilePaths() {
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("aaaa"), List.of("s3://f1")),
                new QuerySplit(split("bbb"), List.of()),
                new QuerySplit(split("cc"), List.of("s3://f3")));

        List<List<String>> paths = pathsFor(splits, 2);

        // Verify no phantom paths appear
        int totalPaths = paths.stream().mapToInt(List::size).sum();
        assertThat(totalPaths).isEqualTo(2); // only f1 and f3
    }

    @Test
    void morePartitionsThanSplits_extraPartitionsEmpty() {
        List<QuerySplit> splits = List.of(
                new QuerySplit(split("aaa"), List.of("s3://f1")));

        List<List<String>> paths = pathsFor(splits, 4);

        assertThat(paths).hasSize(4);
        assertThat(paths.get(0)).containsExactly("s3://f1");
        assertThat(paths.get(1)).isEmpty();
        assertThat(paths.get(2)).isEmpty();
        assertThat(paths.get(3)).isEmpty();
    }

    private static List<List<String>> pathsFor(
            List<QuerySplit> splits, int partitions) {
        var assigned = org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitPartitions
                .partitionSplits(splits, partitions, QuerySplit::estimatedScanBytes).stream()
                .map(partition -> partition.stream().flatMap(split -> split.filePaths().stream()).toList())
                .toList();
        return assigned;
    }
    private static DataSplit split(String id) {
        return DataSplit.newBuilder().setSplitId(id).build();
    }

}
