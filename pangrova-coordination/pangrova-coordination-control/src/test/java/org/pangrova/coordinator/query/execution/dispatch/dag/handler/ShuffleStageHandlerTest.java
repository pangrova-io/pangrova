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
import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveQueryEvaluator;
import org.pangrova.coordinator.query.execution.dispatch.dag.ExchangeInputTestAccess;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.skew.MedianSkewDetector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleStageHandlerTest {

    @Test
    void externalStageFailsBeforeAllocatingWhenNoLifecycleIsConfigured() {
        var lifecycle = org.pangrova.coordinator.query.execution.dispatch.shuffle.UnavailableExternalShuffleLifecycle.INSTANCE;
        var handler = new ShuffleStageHandler(lifecycle);
        Stage producer = stage(3, 2, 100);
        var graph = dag(producer, edge(3, 4, ShuffleType.HASH, 2));
        assertThatThrownBy(() -> handler.onScheduling(event(producer, graph, "q-unavailable")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not configured");
        assertThat(producer.shuffleHandle()).isNull();
    }

    @Test
    void registersAndFinalizesHashOutput() {
        RecordingLifecycle lifecycle = new RecordingLifecycle();
        ShuffleStageHandler handler = new ShuffleStageHandler(lifecycle);
        Stage stage = stage(3, 5, 1_001L);
        StageDAG dag = dag(stage, edge(3, 4, ShuffleType.HASH, 11));
        StageLifecycleEvent event = event(stage, dag, "query-hash");

        handler.onScheduling(event);
        assertThatThrownBy(stage::requireShuffleReceipt).isInstanceOf(IllegalStateException.class);
        handler.onFinished(event);
        assertThat(stage.requireShuffleReceipt().partitions()).hasSize(11);

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
    void registeredExternalOutputCannotScheduleDownstreamBeforeItsReceipt() {
        var producer = stage(3, 1, 100);
        var consumer = stage(4, 4, 100);
        var graph = StageDAG.create(List.of(producer, consumer), List.of(producer.plan(), consumer.plan()),
                Map.of(4, List.of(3)), List.of(edge(3, 4, ShuffleType.HASH, 4)));
        var lifecycleEvent = event(producer, graph, "query-ready");
        var handler = new ShuffleStageHandler(new RecordingLifecycle());
        handler.onScheduling(lifecycleEvent);
        producer.bindShuffleExecutionIdentity(producer.shufflePlanEpoch(),
                org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator").setOwnerIncarnation("incarnation").setFencingToken(1).build());
        producer.stateMachine().tryTransitionTo(org.pangrova.query.api.enums.StageState.SCHEDULING);
        producer.stateMachine().tryTransitionTo(org.pangrova.query.api.enums.StageState.RUNNING);
        assertThat(graph.readyStageEntities(java.util.Set.of(3))).doesNotContain(consumer);
        assertThatThrownBy(() -> ExchangeInputTestAccess
                .build(consumer.plan(), graph, List.of(), 1)).isInstanceOf(IllegalStateException.class);
        handler.onFinished(lifecycleEvent);
        assertThat(graph.readyStageEntities(java.util.Set.of(3))).contains(consumer);
        var advisor = new AdaptiveQueryEvaluator(new MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());
        assertThat(advisor.evaluateStageCompletion(producer).coalesceResult().coalescedCount()).isOne();
        var externalInputs = ExchangeInputTestAccess.buildExternalCohort(consumer.plan(), graph).stream()
                .map(org.pangrova.protocol.v2.ExchangeInputBinding::getExternal).toList();
        assertThat(externalInputs).extracting(ExternalShuffleInput::getSourcePartitionStart)
                .containsExactly(0, 1, 2, 3);
        assertThat(externalInputs).allSatisfy(input ->
                assertThat(input.getEndpoints(0)).isSameAs(externalInputs.get(0).getEndpoints(0)));
        var bound = externalInputs.get(1);
        var expected = producer.requireShuffleReceipt().partitions().get(1);
        assertThat(bound.getSourcePartitionStart()).isEqualTo(1);
        assertThat(bound.getPublications(0).getLastSequence()).isEqualTo(expected.lastSequence());
        assertThat(bound.getPublications(0).getTotalBytes()).isEqualTo(expected.bytes());
        assertThat(bound.getPublications(0).getTotalRows()).isEqualTo(expected.rows());
        assertThat(bound.getPublications(0).getManifestSha256().toByteArray())
                .isEqualTo(java.util.HexFormat.of().parseHex(expected.manifestSha256()));

    }

    @Test
    void coalescedConsumerPreservesAllFourPublicationsAndRegistersOneProducer() {
        var lifecycle = new RecordingLifecycle();
        var handler = new ShuffleStageHandler(lifecycle);
        var producer = stage(3, 1, 100);
        var input = new StageEdge(3, 4, ShuffleType.HASH, List.of(0), "hash-input", 4,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
        var source = producer.plan().stagePlan().requireLiveFragment().relNode();
        var scan = new PangrovaExchangeInput(source.getCluster(), source.getTraitSet(), source.getRowType(),
                new PangrovaExchangeInput.Specification("exchange-input-hash-input", input.exchangeId(),
                        3, 4, PangrovaDistribution.hash(List.of(0)), 4, input.batchSerde(), true));
        var consumer = Stage.create(new StageNode(4, new PlanFragment(scan, 100, 4),
                4, "COMPUTE", List.of()), "q-coalesced");
        var downstream = Stage.create(new StageNode(5, plan(), 1, "OUTPUT", List.of()), "q-coalesced");
        var output = edge(4, 5, ShuffleType.SINGLETON, 1);
        var graph = StageDAG.create(List.of(producer, consumer, downstream),
                List.of(producer.plan(), consumer.plan(), downstream.plan()),
                Map.of(4, List.of(3), 5, List.of(4)), List.of(input, output));
        var producerEvent = event(producer, graph, "q-coalesced");
        handler.onScheduling(producerEvent);
        producer.bindShuffleExecutionIdentity(producer.shufflePlanEpoch(),
                org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator").setOwnerIncarnation("incarnation").setFencingToken(1).build());
        handler.onFinished(producerEvent);

        ExchangeInputTestAccess.freezeCoalescedInputs(consumer, graph);
        handler.onScheduling(event(consumer, graph, "q-coalesced"));
        assertThat(consumer.physicalTaskCount()).isEqualTo(1);
        assertThat(lifecycle.registrations.get(1).expectedProducerCount()).isEqualTo(1);
        assertThat(input.partitionCount()).isEqualTo(4);
        var bindings = ExchangeInputTestAccess.buildExternalCohort(consumer.plan(), graph);
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getExternal().getSourcePartitionStart()).isZero();
        assertThat(bindings.get(0).getExternal().getPublicationsList())
                .extracting(org.pangrova.protocol.v2.ShufflePartitionPublication::getTotalBytes)
                .containsExactly(16L, 17L, 18L, 19L);
        assertThatThrownBy(() -> consumer.freezeCoalescedInputs(
                org.pangrova.planner.api.adaptive.CoalesceResult.NO_OP)).isInstanceOf(IllegalStateException.class);
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
        handler.onFinished(event);
        var oldReceipt = stage.requireShuffleReceipt();
        byte[] initialChecksum = lifecycle.registrations.get(0)
                .expectedProducerSetChecksum();
        stage.stateMachine().transitionTo(org.pangrova.query.api.enums.StageState.SCHEDULING);
        stage.stateMachine().transitionTo(org.pangrova.query.api.enums.StageState.RUNNING);
        stage.addTaskId("failed");
        stage.onTaskFailed("failed", new org.pangrova.query.api.error.QueryFailure("WORKER_LOST", "worker", "lost"));
        assertThat(stage.tryRestartFailedGeneration(0, 1, "WORKER_LOST")).isTrue();
        handler.onScheduling(event);

        assertThatThrownBy(stage::requireShuffleReceipt).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stage.bindShuffleReceipt(oldReceipt.publication(), oldReceipt))
                .isInstanceOf(IllegalStateException.class);
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
                new org.pangrova.execution.authority.StageExecutionContext.Live(
                        new org.pangrova.query.api.session.QuerySession(queryId, Map.of(), null, "tenant-test", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())));
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
                partitions, BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
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
        public ShuffleCapabilityDescriptor capabilities() {
            return ShuffleCapabilityDescriptor.none();
        }

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
        public ShuffleCommitReceipt commitShuffle(ShufflePublication publication) {
            commits.add(publication.queryId() + ":" + publication.stageId());
            var registration = registrations.get(registrations.size() - 1);
            var partitions = new java.util.HashMap<Integer, ShuffleCommitReceipt.Partition>();
            for (int id = 0; id < registration.numPartitions(); id++) {
                String digest = "%064x".formatted(id);
                partitions.put(id, new ShuffleCommitReceipt.Partition(0, 16 + id, 1, digest));
            }
            return new ShuffleCommitReceipt(publication, registration.expectedProducerCount(),
                    java.util.HexFormat.of().formatHex(registration.expectedProducerSetChecksum()), partitions);
        }

        @Override
        public void cleanupShuffle(String queryId) {}
    }
}
