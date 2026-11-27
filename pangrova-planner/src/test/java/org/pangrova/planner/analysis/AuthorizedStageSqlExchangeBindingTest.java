package org.pangrova.planner.analysis;

import org.pangrova.planner.enums.StageTerminalPurpose;

import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.stream.Collectors;

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
        var fragmented = PlanFragmenter.fragment(exchange, 1, StageTerminalPurpose.RELATIONAL_EXPORT);
        var graph = AuthorizedStageGraph.compile(fragmented, compiler, compiled.preOptimizationColumnNames(),
                StageTerminalPurpose.RELATIONAL_EXPORT);
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

    @Test
    void chainedExchangesUseTheBoundIntermediateProducerSchema() throws Exception {
        var purpose = StageTerminalPurpose.RELATIONAL_EXPORT;
        var compiler = new SqlCompiler();
        var compiled = compiler.compileForPolicyApplication("SELECT 1 + 2 AS total");
        var node = compiled.plan();
        var first = new PangrovaExchange(node.getCluster(), node.getTraitSet(), node,
                PangrovaDistribution.SINGLETON, 1);
        var second = new PangrovaExchange(first.getCluster(), first.getTraitSet(), first,
                PangrovaDistribution.SINGLETON, 1);
        var graph = AuthorizedStageGraph.compile(PlanFragmenter.fragment(second, 1, purpose),
                compiler, compiled.preOptimizationColumnNames(), purpose);
        assertThat(graph.transfers()).hasSize(2);

        var roots = new java.util.HashMap<Integer, io.substrait.proto.RelRoot>();
        for (var stage : graph.stages()) {
            var plan = Plan.parseFrom(stage.serializedPlan().planBytes());
            roots.put(stage.stageId(), plan.getRelations(0).getRoot());
        }
        for (var edge : graph.transfers()) {
            var producer = roots.get(edge.upstreamStageId()).getInput();
            var consumer = roots.get(edge.downstreamStageId()).getInput();
            var leaf = consumer.hasExchange() ? consumer.getExchange().getInput() : consumer;
            var input = leaf.getExtensionLeaf().getDetail().unpack(RelationDetailEnvelope.class).getExchangeInput();
            assertThat(input.getExchangeId()).isEqualTo(edge.exchangeId());
            assertThat(input.getOutputSchema()).isEqualTo(SubstraitRelationLayout.describe(producer).outputSchema());
        }
        AuthorizedStageGraph.validateExchangeContracts(graph.stages().stream().collect(Collectors.toMap(
                AuthorizedStageGraph.SerializedStage::stageId, AuthorizedStageGraph.SerializedStage::serializedPlan)),
                graph.transfers());
    }
}
