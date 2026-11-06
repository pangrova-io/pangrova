package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.invocation.AiInvocationPlanHint;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.ExchangeRel;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.analysis.AuthorizedStageGraph.SerializedStage;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.planner.core.type.PlannerTypeFactory;
import org.pangrova.connector.api.metadata.Constraint;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Structural replay and identity checks use real serialized Substrait, without a second SQL execution path. */
class AuthorizedStageGraphTest {
    @Test
    void canonicalizesOrderingAndPreservesMultiplePhysicalSources() throws Exception {
        var first = edge(1, 0, "left-gather");
        var second = edge(2, 0, "right-gather");
        var left = stage(1, 4, "COMPUTE", "left", producerPlan());
        var right = stage(2, 4, "COMPUTE", "right", producerPlan());
        var output = stage(0, 1, "OUTPUT", null, consumerPlan(List.of(first, second)));
        var graph = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(right, output, left), List.of(second, first));
        var reordered = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, left, right), List.of(first, second));
        assertThat(graph.stages()).extracting(SerializedStage::stageId).containsExactly(0, 1, 2);
        assertThat(graph.stages().stream().flatMap(stage -> stage.sources().stream()).toList())
                .extracting(SplitSourceDescriptor::qualifiedName).containsExactly("lake.raw.left", "lake.raw.right");
        assertThat(graph.sha256()).isEqualTo(reordered.sha256());
        byte[] bytes = graph.stages().get(0).serializedPlan().planBytes();
        bytes[0] ^= 1;
        assertThat(graph.sha256()).isEqualTo(reordered.sha256());
    }

    @Test
    void identityIncludesTypedInvocationIntentsAndOutputNames() throws Exception {
        var bytes = plan().planBytes();
        var plain = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(stage(0, 1, "OUTPUT", null,
                new SerializedPlanFragment(bytes, List.of(), List.of()))), List.of());
        var governed = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(stage(0, 1, "OUTPUT", null,
                new SerializedPlanFragment(bytes, List.of(), List.of(new AiInvocationPlanHint(
                        "invocation-1", "ai_embed", "registered-model",
                        AiInvocationPlanHint.Operation.EMBEDDING, false))))), List.of());
        var renamed = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(new SerializedStage(0, 1, "OUTPUT", plan(),
                List.of(), List.of("renamed"), List.of(), List.of(), -1, -1)), List.of());
        assertThat(plain.sha256()).isNotEqualTo(governed.sha256()).isNotEqualTo(renamed.sha256());
    }

    @Test
    void graphIdentityRetainsDimensionsThatCoreSubstraitListsCannotEncode() throws Exception {
        var types = new PlannerTypeFactory();
        var rex = new RexBuilder(types);
        var cluster = RelOptCluster.create(new VolcanoPlanner(), rex);
        var row = types.builder().add("embedding", types.createStorageVector(VectorLogicalType.create(3))).build();
        var values = LogicalValues.createEmpty(cluster, row);
        var serialized = new SerializedPlanFragment(new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(values)), List.of(), List.of());
        var firstType = ResultColumnLogicalType.newBuilder().setColumnIndex(0)
                .setLogicalType(VectorLogicalType.create(3)).build();
        var secondType = firstType.toBuilder().setLogicalType(VectorLogicalType.create(4)).build();
        var first = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.QUERY_RESULT,
                List.of(new SerializedStage(0, 1, "OUTPUT", serialized, List.of(), List.of("embedding"),
                        List.of(), List.of(firstType), -1, -1)), List.of());
        var second = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.QUERY_RESULT,
                List.of(new SerializedStage(0, 1, "OUTPUT", serialized, List.of(), List.of("embedding"),
                        List.of(), List.of(secondType), -1, -1)), List.of());
        assertThat(first.sha256()).isNotEqualTo(second.sha256());
        assertThat(first.stages().get(0).sha256()).isNotEqualTo(second.stages().get(0).sha256());
        assertThat(first.stages().get(0).resultColumnTypes()).containsExactly(firstType);
    }

    @Test
    void rejectsCyclesDetachedStagesMissingExchangeBindingsAndNonCompleteEdges() throws Exception {
        var output = stage(0, 1, "OUTPUT", null, plan());
        var first = stage(1, 1, "COMPUTE", "left", plan());
        var second = stage(2, 1, "COMPUTE", "right", plan());
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, first), List.of()))
                .hasMessageContaining("detached");
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, first), List.of(edge(3, 0, "missing"))))
                .hasMessageContaining("invalid exchange");
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, first, second),
                List.of(edge(1, 2, "forward"), edge(2, 1, "backward"), edge(2, 0, "terminal"))))
                .hasMessageContaining("cycle");
        var partial = new StageTransfer(1, 0, ShuffleType.SINGLETON, List.of(), "partial", 1,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, first), List.of(partial)))
                .hasMessageContaining("invalid exchange");
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, output), List.of()))
                .hasMessageContaining("Duplicate");
    }

    private static SerializedStage stage(int id, int parallelism, String role, String source,
            SerializedPlanFragment plan) throws Exception {
        List<SplitSourceDescriptor> sources = source == null ? List.of() : List.of(
                new SplitSourceDescriptor("lake", "raw", source, Constraint.EMPTY, List.of("id"), java.util.Optional.empty()));
        var schema = SubstraitRelationLayout.rootSchema(Plan.parseFrom(plan.planBytes()).getRelations(0).getRoot());
        return new SerializedStage(id, parallelism, role, plan, sources,
                SubstraitRelationLayout.topLevelNames(schema), List.of(), List.of(), -1, -1);
    }

    private static SerializedPlanFragment producerPlan() throws Exception {
        Plan source = Plan.parseFrom(plan().planBytes());
        var root = source.getRelations(0).getRoot();
        var exchange = Rel.newBuilder().setExchange(ExchangeRel.newBuilder().setInput(root.getInput())
                .setPartitionCount(1).setRoundRobin(ExchangeRel.RoundRobin.newBuilder().setExact(true))).build();
        return new SerializedPlanFragment(source.toBuilder().setRelations(0, source.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(exchange))).build().toByteArray(), List.of(), List.of());
    }

    private static SerializedPlanFragment consumerPlan(List<StageTransfer> edges) throws Exception {
        Plan source = Plan.parseFrom(plan().planBytes());
        var schema = SubstraitRelationLayout.describe(source.getRelations(0).getRoot().getInput()).outputSchema();
        List<Rel> inputs = edges.stream().map(edge -> {
            var detail = ExchangeInputDetail.newBuilder().setPlanNodeId("input-" + edge.exchangeId())
                    .setExchangeId(edge.exchangeId()).setUpstreamStageId(edge.upstreamStageId())
                    .setDownstreamStageId(edge.downstreamStageId()).setPartitionCount(edge.partitionCount())
                    .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                    .setBatchSerde(edge.batchSerde()).setRequireComplete(true).setOutputSchema(schema).build();
            return Rel.newBuilder().setExtensionLeaf(io.substrait.proto.ExtensionLeafRel.newBuilder()
                    .setDetail(Any.pack(RelationDetailEnvelope.newBuilder().setSchemaVersion(1)
                            .setExchangeInput(detail).build()))).build();
        }).toList();
        Rel root = Rel.newBuilder().setCross(io.substrait.proto.CrossRel.newBuilder()
                .setLeft(inputs.get(0)).setRight(inputs.get(1))).build();
        return new SerializedPlanFragment(source.toBuilder().setRelations(0, source.getRelations(0).toBuilder()
                .setRoot(source.getRelations(0).getRoot().toBuilder().setInput(root).clearNames()
                        .addNames("left_id").addNames("right_id"))).build().toByteArray(), List.of(), List.of());
    }

    private static StageTransfer edge(int source, int target, String id) {
        return new StageTransfer(source, target, ShuffleType.SINGLETON, List.of(), id, 1,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
    }

    private static SerializedPlanFragment plan() throws Exception {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var cluster = RelOptCluster.create(new VolcanoPlanner(), rex);
        var rowType = types.builder().add("id", SqlTypeName.INTEGER).build();
        var values = LogicalValues.create(cluster, rowType, ImmutableList.of(ImmutableList.of(
                rex.makeExactLiteral(BigDecimal.ONE, types.createSqlType(SqlTypeName.INTEGER)))));
        return new SerializedPlanFragment(new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(values)), List.of(), List.of());
    }
}
