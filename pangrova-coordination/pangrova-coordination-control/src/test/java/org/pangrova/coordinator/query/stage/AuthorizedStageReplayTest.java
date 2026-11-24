package org.pangrova.coordinator.query.stage;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.planner.enums.StageTerminalPurpose;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitFetcher;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.analysis.AuthorizedStageGraph.SerializedStage;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.analysis.StagePlanAccess;
import org.pangrova.planner.analysis.StageTransfer;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import com.google.protobuf.Any;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.ExchangeRel;
import org.pangrova.security.api.identity.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizedStageReplayTest {
    @Test
    void restoresOpaqueStagesWithTheAuthorizedCoordinates() throws Exception {
        var graph = graph();
        var dag = StageDAG.fromAuthorizedStageGraph(graph).withStages("replay-query");
        assertThat(dag.stages()).hasSize(2);
        assertThat(dag.edges().get(0)).containsExactly(1);
        for (var stage : dag.stages()) {
            assertThat(stage.stagePlan().mode()).isEqualTo(StagePlanMode.DURABLE_REPLAY);
            assertThat(stage.stagePlan().toSerializedPlan(null, stage.stagePlan().fieldNames()))
                    .isEqualTo(((SerializedStage) stage.stagePlan()).serializedPlan());
            assertThatThrownBy(stage.stagePlan()::requireLiveFragment).hasMessageContaining("no live planner");
            assertCoordinatesEnforced(stage.stagePlan(), stage.stageId(), stage.parallelism(), stage.taskType());
        }
    }

    @Test
    void replayRejectsEveryNonTransactionalAllocationAndCatalogLookup() throws Exception {
        var graph = graph();
        var dag = StageDAG.fromAuthorizedStageGraph(graph);
        var context = QueryExecutionScope.readOnly(new QuerySession("replay-query", Map.of(),
                new UserIdentity("authenticated-owner", Set.of(), Map.of(), Instant.EPOCH), "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
        var fetcher = new StageSplitFetcher(null, null);
        assertThatThrownBy(() -> new StageSplitFetcher(null, null).fetch(
                dag.findStageNode(0), context.requireLiveSession(), context.snapshotOverrides(), null))
                .hasMessageContaining("exact persisted partition gate");
        var original = graph.stages().get(0);
        var changed = new SerializedStage(0, original.parallelism(), "OUTPUT", original.serializedPlan(),
                original.sources(), List.of("different_output"), List.of(), List.of(), -1, -1);
        assertThatThrownBy(() -> fetcher.fetch(new StageNode(0, changed, 1, "OUTPUT", List.of()),
                context.requireLiveSession(), context.snapshotOverrides(), null))
                .hasMessageContaining("exact persisted partition gate");
        assertThatThrownBy(() -> fetcher.fetch(dag.findStageNode(1),
                context.requireLiveSession(), context.snapshotOverrides(), null))
                .hasMessageContaining("exact persisted partition gate");
    }

    @Test
    void everyReplayImplementationRejectsAlteredFrozenCoordinates() throws Exception {
        var serialized = graph().stages().get(0);
        assertCoordinatesEnforced(serialized, serialized.stageId(), serialized.parallelism(), serialized.taskType());
        var template = org.pangrova.generation.api.graph.template.GenerationStageTemplate.of(
                new byte[0], List.of(), List.of());
        var persisted = org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan.of(
                3, 2, org.pangrova.generation.api.enums.GenerationPageKind.RELATIONAL_COMPUTE,
                serialized.serializedPlan().planBytes(), template, List.of(), serialized.outputNames(), false);
        var replay = new org.pangrova.coordinator.generation.template.PersistedStagePlan(persisted);
        assertCoordinatesEnforced(replay, 3, 2, "COMPUTE");
    }

    @Test
    void replayWithoutFrozenCoordinatesIsRejectedBeforeDispatch() {
        var replay = (StagePlanAccess) java.lang.reflect.Proxy.newProxyInstance(
                StagePlanAccess.class.getClassLoader(), new Class<?>[]{StagePlanAccess.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "mode" -> StagePlanMode.DURABLE_REPLAY;
                    case "frozenCoordinates" -> java.util.Optional.empty();
                    default -> throw new AssertionError("Unexpected stage access: " + method);
                });
        assertThatThrownBy(() -> new StageNode(1, replay, 2, "COMPUTE", List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no frozen execution coordinates");
    }

    private static void assertCoordinatesEnforced(StagePlanAccess plan, int stage, int parallelism, String type) {
        assertThat(new StageNode(stage, plan, parallelism, type, List.of()).stagePlan()).isSameAs(plan);
        assertThatThrownBy(() -> new StageNode(stage + 1, plan, parallelism, type, List.of()))
                .hasMessageContaining("immutable graph identity");
        assertThatThrownBy(() -> new StageNode(stage, plan, parallelism + 1, type, List.of()))
                .hasMessageContaining("immutable graph identity");
        assertThatThrownBy(() -> new StageNode(stage, plan, parallelism, "OTHER", List.of()))
                .hasMessageContaining("immutable graph identity");
    }

    private static AuthorizedStageGraph graph() throws Exception {
        var live = live();
        var plan = Plan.parseFrom(new SubstraitPlanSerializer().serialize(new SubstraitConverter().convert(live.relNode())));
        var root = plan.getRelations(0).getRoot();
        var schema = SubstraitRelationLayout.describe(root.getInput()).outputSchema();
        var detail = ExchangeInputDetail.newBuilder().setPlanNodeId("gather-input").setExchangeId("gather")
                .setUpstreamStageId(1).setDownstreamStageId(0).setPartitionCount(1)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                .setBatchSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH).setRequireComplete(true).setOutputSchema(schema).build();
        var input = Rel.newBuilder().setExtensionLeaf(io.substrait.proto.ExtensionLeafRel.newBuilder()
                .setDetail(Any.pack(RelationDetailEnvelope.newBuilder().setSchemaVersion(1).setExchangeInput(detail).build())));
        var exchange = Rel.newBuilder().setExchange(ExchangeRel.newBuilder().setInput(root.getInput())
                .setPartitionCount(1).setRoundRobin(ExchangeRel.RoundRobin.newBuilder().setExact(true)));
        var output = new SerializedStage(0, 1, "OUTPUT", serialized(plan, input.build()),
                List.of(), live.fieldNames(), List.of(), List.of(), -1, -1);
        var producer = new SerializedStage(1, 4, "COMPUTE", serialized(plan, exchange.build()),
                List.of(), live.fieldNames(), List.of(), List.of(), -1, -1);
        return new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, producer), List.of(new StageTransfer(1, 0,
                ShuffleType.SINGLETON, List.of(), "gather", 1, BatchSerde.BATCH_SERDE_ARROW_BATCH, true)));
    }

    private static SerializedPlanFragment serialized(Plan plan, Rel root) {
        return new SerializedPlanFragment(plan.toBuilder().setRelations(0, plan.getRelations(0).toBuilder()
                .setRoot(plan.getRelations(0).getRoot().toBuilder().setInput(root))).build().toByteArray(), List.of(), List.of());
    }

    private static PlanFragment live() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        return new PlanFragment(LogicalValues.createOneRow(cluster), -1, -1);
    }
}
