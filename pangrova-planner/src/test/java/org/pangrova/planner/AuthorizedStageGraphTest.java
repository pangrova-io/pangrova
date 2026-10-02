package org.pangrova.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.AuthorizedStageGraph.SerializedStage;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.spi.connector.metadata.Constraint;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Structural replay and identity checks use real serialized Substrait, without a second SQL execution path. */
class AuthorizedStageGraphTest {
    @Test
    void canonicalizesOrderingAndPreservesMultiplePhysicalSources() throws Exception {
        var left = stage(1, 4, "COMPUTE", "left", plan());
        var right = stage(2, 4, "COMPUTE", "right", plan());
        var output = stage(0, 1, "OUTPUT", null, plan());
        var first = edge(1, 0, "left-gather");
        var second = edge(2, 0, "right-gather");
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
                List.of(), List.of("renamed"), List.of(), -1, -1)), List.of());
        assertThat(plain.sha256()).isNotEqualTo(governed.sha256()).isNotEqualTo(renamed.sha256());
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
                BatchSerde.BATCH_SERDE_NATIVE_ROW, false);
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, first), List.of(partial)))
                .hasMessageContaining("invalid exchange");
        assertThatThrownBy(() -> new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(output, output), List.of()))
                .hasMessageContaining("Duplicate");
    }

    private static SerializedStage stage(int id, int parallelism, String role, String source,
            SerializedPlanFragment plan) {
        List<SplitSourceDescriptor> sources = source == null ? List.of() : List.of(
                new SplitSourceDescriptor("lake", "raw", source, Constraint.EMPTY, List.of("id")));
        return new SerializedStage(id, parallelism, role, plan, sources, List.of("id"), List.of(), -1, -1);
    }

    private static StageTransfer edge(int source, int target, String id) {
        return new StageTransfer(source, target, ShuffleType.SINGLETON, List.of(), id, 1,
                BatchSerde.BATCH_SERDE_NATIVE_ROW, true);
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
