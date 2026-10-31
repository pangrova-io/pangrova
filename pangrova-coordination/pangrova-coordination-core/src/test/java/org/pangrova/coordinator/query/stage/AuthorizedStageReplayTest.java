package org.pangrova.coordinator.query.stage;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveStagePlanResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.split.StageSplitFetcher;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.AuthorizedStageGraph.SerializedStage;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.ShuffleType;
import org.pangrova.planner.StagePlanAccess;
import org.pangrova.planner.StageTransfer;
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
import org.pangrova.security.api.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizedStageReplayTest {
    @Test
    void restoresOpaqueStagesWithoutPlannerCapabilityOrAdaptiveRebinding() throws Exception {
        var graph = graph();
        var dag = StageDAG.fromAuthorizedStageGraph(graph).withStages("replay-query");
        assertThat(dag.stages()).hasSize(2);
        assertThat(dag.edges().get(0)).containsExactly(1);
        for (var stage : dag.stages()) {
            assertThat(stage.stagePlan().mode()).isEqualTo(StagePlanAccess.Mode.DURABLE_REPLAY);
            assertThat(stage.stagePlan().toSerializedPlan(null, stage.stagePlan().fieldNames()))
                    .isEqualTo(((SerializedStage) stage.stagePlan()).serializedPlan());
            assertThat(new AdaptiveStagePlanResolver(null).apply(stage, dag, null)).isSameAs(stage);
            assertThatThrownBy(stage.stagePlan()::requireLiveFragment).hasMessageContaining("no live planner");
            assertThatThrownBy(() -> stage.withParallelism(stage.parallelism() + 1))
                    .hasMessageContaining("persisted graph identity");
            assertThatThrownBy(() -> stage.withStagePlan(live()))
                    .hasMessageContaining("cannot be adaptively replaced");
        }
    }

    @Test
    void replayRejectsEveryNonTransactionalAllocationAndCatalogLookup() throws Exception {
        var graph = graph();
        var dag = StageDAG.fromAuthorizedStageGraph(graph);
        var context = DAGQueryContext.readOnly(new CoordinatorSessionContext("replay-query", Map.of(),
                new UserIdentity("authenticated-owner", Set.of(), Map.of(), Instant.EPOCH), "tenant-a"));
        var fetcher = new StageSplitFetcher(null, null);
        assertThatThrownBy(() -> new StageSplitFetcher(null, null).fetch(
                dag.findStageNode(0), dag, "replay-query", context))
                .hasMessageContaining("exact persisted partition gate");
        var original = graph.stages().get(0);
        var changed = new SerializedStage(0, original.parallelism(), "OUTPUT", original.serializedPlan(),
                original.sources(), List.of("different_output"), List.of(), List.of(), -1, -1);
        assertThatThrownBy(() -> fetcher.fetch(new StageNode(0, changed, 1, "OUTPUT", List.of()),
                dag, "replay-query", context)).hasMessageContaining("exact persisted partition gate");
        assertThatThrownBy(() -> fetcher.fetch(dag.findStageNode(1), dag, "replay-query", context))
                .hasMessageContaining("exact persisted partition gate");
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
        return new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
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
