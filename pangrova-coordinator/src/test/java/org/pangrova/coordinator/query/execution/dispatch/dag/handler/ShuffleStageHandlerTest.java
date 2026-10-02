package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.ShuffleType;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleServiceAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleStageHandlerTest {

    @Test
    void registersAndFinalizesHashOutput() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ShuffleStageHandler handler = new ShuffleStageHandler(lifecycle);
        Stage stage = stage(3, 5, 1_001L);
        StageDAG dag = dag(stage, edge(3, 4, ShuffleType.HASH, 11));
        StageLifecycleEvent event = event(stage, dag, "query-hash");

        handler.onScheduling(event);
        handler.onFinished(event);

        assertThat(lifecycle.registrations).hasSize(1);
        ShuffleRegistration registration = lifecycle.registrations.get(0);
        assertThat(registration.numPartitions()).isEqualTo(11);
        assertThat(registration.expectedProducerCount()).isEqualTo(5);
        assertThat(registration.estimatedBytesPerPartition()).isEqualTo(91L);
        assertThat(registration.planEpoch()).isEqualTo(1L);
        assertThat(stage.shufflePlanEpoch()).isEqualTo(1L);
        assertThat(lifecycle.commits).containsExactly("query-hash:3");
    }

    @Test
    void registersAndFinalizesSingletonAndRoundRobinWithExactProducerCohort() {
        for (var distribution : List.of(ShuffleType.SINGLETON, ShuffleType.ROUND_ROBIN)) {
            RecordingLifecycle lifecycle = new RecordingLifecycle();
            var handler = new ShuffleStageHandler(lifecycle);
            Stage producer = stage(3, 5, -1L);
            int partitions = distribution == ShuffleType.SINGLETON ? 1 : 4;
            var graph = dag(producer, edge(3, 4, distribution, partitions));
            var lifecycleEvent = event(producer, graph, "query-" + distribution);
            handler.onScheduling(lifecycleEvent);
            handler.onFinished(lifecycleEvent);
            assertThat(lifecycle.registrations).hasSize(1);
            assertThat(lifecycle.registrations.get(0).numPartitions()).isEqualTo(partitions);
            assertThat(lifecycle.registrations.get(0).expectedProducerCount()).isEqualTo(5);
            assertThat(lifecycle.commits).containsExactly("query-" + distribution + ":3");
        }
    }

    @Test
    void ignoresTerminalAndUnsupportedBroadcastOutputs() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ShuffleStageHandler handler = new ShuffleStageHandler(lifecycle);
        Stage terminal = stage(1, 2, -1L);
        Stage broadcast = stage(2, 2, -1L);

        handler.onScheduling(event(terminal, dag(terminal), "query-terminal"));
        handler.onFinished(event(terminal, dag(terminal), "query-terminal"));
        StageDAG broadcastDag = dag(
                broadcast, edge(2, 3, ShuffleType.BROADCAST, 4));
        handler.onScheduling(event(broadcast, broadcastDag, "query-broadcast"));
        handler.onFinished(event(broadcast, broadcastDag, "query-broadcast"));

        assertThat(lifecycle.registrations).isEmpty();
        assertThat(lifecycle.commits).isEmpty();
        assertThat(terminal.shuffleHandle()).isNull();
        assertThat(broadcast.shuffleHandle()).isNull();
    }

    @Test
    void rejectsMultipleHashOutputsBecauseStageHasOneAllocation() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ShuffleStageHandler handler = new ShuffleStageHandler(lifecycle);
        Stage stage = stage(7, 2, -1L);
        StageDAG dag = dag(stage,
                edge(7, 8, ShuffleType.HASH, 3),
                edge(7, 9, ShuffleType.HASH, 3));

        assertThatThrownBy(() -> handler.onScheduling(event(stage, dag, "query-fanout")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("multiple external shuffle outputs");
        assertThat(lifecycle.registrations).isEmpty();
    }

    @Test
    void retryRebindsAUniquePlanEpochAndProducerSet() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ShuffleStageHandler handler = new ShuffleStageHandler(lifecycle);
        Stage stage = stage(5, 3, -1L);
        StageDAG dag = dag(stage, edge(5, 6, ShuffleType.HASH, 8));
        StageLifecycleEvent event = event(stage, dag, "query-retry");

        handler.onScheduling(event);
        byte[] initialChecksum = lifecycle.registrations.get(0)
                .expectedProducerSetChecksum();
        stage.tryPrepareRetry();
        handler.onScheduling(event);

        assertThat(lifecycle.registrations).hasSize(2);
        assertThat(lifecycle.registrations.get(1).planEpoch()).isEqualTo(2L);
        assertThat(Arrays.equals(
                initialChecksum,
                lifecycle.registrations.get(1).expectedProducerSetChecksum()))
                .isFalse();
        assertThat(stage.shufflePlanEpoch()).isEqualTo(2L);
    }

    private static StageLifecycleEvent event(
            Stage stage, StageDAG dag, String queryId) {
        return new StageLifecycleEvent(stage, dag, queryId,
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(
                        new org.pangrova.coordinator.query.CoordinatorSessionContext(queryId, Map.of(), null, "tenant-test")));
    }

    private static StageDAG dag(Stage stage, StageEdge... edges) {
        List<StageNode> nodes = new ArrayList<>();
        nodes.add(stage.plan());
        for (StageEdge edge : edges) {
            nodes.add(new StageNode(
                    edge.downstreamStageId(), plan(), 1, "OUTPUT", List.of()));
        }
        return StageDAG.create(
                List.of(stage), nodes, Map.of(), List.of(edges));
    }

    private static Stage stage(int stageId, int parallelism, long estimatedBytes) {
        return Stage.create(new StageNode(
                stageId, plan(), parallelism, "SCAN", List.of(),
                estimatedBytes, -1L), "unused-query-id");
    }

    private static StageEdge edge(
            int upstream, int downstream, ShuffleType type, int partitions) {
        return new StageEdge(
                upstream, downstream, type, List.of(),
                "exchange-" + upstream + "-" + downstream,
                partitions, BatchSerde.BATCH_SERDE_NATIVE_ROW, true);
    }

    private static PlanFragment plan() {
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        return new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
    }

    private static final class RecordingLifecycle implements ShuffleLifecycleManager {
        private final List<ShuffleRegistration> registrations = new ArrayList<>();
        private final List<String> commits = new ArrayList<>();

        @Override
        public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
            registrations.add(registration);
            int partitions = registration.numPartitions();
            long expiry = System.currentTimeMillis() + 300_000L;
            return new ShuffleHandle(
                    "shuffle-" + registration.stageId() + "-" + registration.planEpoch(),
                    List.of(new ShuffleServerEndpoint(
                            "shuffle", 9090, 0, partitions, "")),
                    new ShuffleServiceAddress("shuffle", 9090, ""),
                    new ShuffleAccess(new byte[] {1}, expiry),
                    new ShuffleAccess(new byte[] {2}, expiry),
                    new ShuffleAccess(new byte[] {3}, expiry),
                    new ShuffleAccess(new byte[] {4}, expiry),
                    registration.expectedProducerCount(),
                    registration.expectedProducerSetChecksum());
        }

        @Override
        public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
            commits.add(queryId + ":" + stageId);
            return ShuffleCommitResult.EMPTY;
        }

        @Override
        public void cleanupShuffle(String queryId) {}
    }
}
