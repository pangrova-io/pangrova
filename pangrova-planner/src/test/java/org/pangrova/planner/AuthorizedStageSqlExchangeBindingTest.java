package org.pangrova.planner;

import io.substrait.proto.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL -> fragment -> generated protocol; intentionally not a Native value/transport oracle. */
class AuthorizedStageSqlExchangeBindingTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT x + 1 AS \"total\" FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x) t",
            "SELECT SUM(x) AS \"total\" FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x) t"
    })
    void canonicalComputedAndAggregateWireNamesComeFromTheActualProducer(String sql) throws Exception {
        var compiler = new SqlCompiler();
        var compiled = compiler.compileForPolicyApplication(sql);
        var node = compiled.plan();
        var exchange = new PangrovaExchange(node.getCluster(), node.getTraitSet(), node,
                PangrovaDistribution.SINGLETON, 1);
        var fragmented = PlanFragmenter.fragment(exchange, 1, AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT);
        var graph = AuthorizedStageGraph.compile(fragmented, compiler, compiled.preOptimizationColumnNames(),
                AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT);
        assertThat(graph.transfers()).hasSize(1);
        var edge = graph.transfers().get(0);
        var source = graph.stages().stream().filter(stage -> stage.stageId() == edge.upstreamStageId()).findFirst().orElseThrow();
        var output = graph.stages().stream().filter(stage -> stage.stageId() == edge.downstreamStageId()).findFirst().orElseThrow();
        var producerRoot = Plan.parseFrom(source.serializedPlan().planBytes()).getRelations(0).getRoot();
        var consumerRoot = Plan.parseFrom(output.serializedPlan().planBytes()).getRelations(0).getRoot();
        var expected = SubstraitRelationLayout.describe(producerRoot.getInput()).outputSchema();
        var detail = consumerRoot.getInput().getExtensionLeaf().getDetail().unpack(RelationDetailEnvelope.class)
                .getExchangeInput();
        assertThat(detail.getOutputSchema()).isEqualTo(expected);
        assertThat(expected.getNamesList()).isNotEqualTo(producerRoot.getNamesList());
        assertThat(consumerRoot.getNamesList()).containsExactly("total");
        assertThat(output.outputNames()).containsExactly("total");
        assertThat(graph.stages()).allSatisfy(stage -> assertThat(stage.serializedPlan().planBytes()).isNotEmpty());
        assertThat(new AuthorizedStageGraph(graph.terminalPurpose(), graph.stages(), graph.transfers()).sha256())
                .isEqualTo(graph.sha256());
    }
}
