package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.security.api.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual live DAG serialization, not a Native transport or value oracle. */
class DAGStageExchangeBindingTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT x + 1 AS \"total\" FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x) t",
            "SELECT SUM(x) AS \"total\" FROM (SELECT 2 AS x UNION ALL SELECT 3 AS x) t",
            "SELECT a.id, b.id FROM (SELECT 1 AS id) a JOIN (SELECT 1 AS id) b ON a.id = b.id"
    })
    void liveExchangeUsesActualProducerSchemaInsteadOfPresentationNames(String sql) throws Exception {
        var fixture = fixture(sql);
        var prepared = fixture.state().serializePlan(fixture.producer(), fixture.context());
        recordDispatchedProducer(fixture, prepared.planBytes());
        var source = Plan.parseFrom(prepared.planBytes()).getRelations(0).getRoot();
        var expected = SubstraitRelationLayout.describe(source.getInput()).outputSchema();
        var output = Plan.parseFrom(fixture.state().serializePlan(fixture.consumer(), fixture.context()).planBytes())
                .getRelations(0).getRoot();
        var input = output.getInput().getExtensionLeaf().getDetail()
                .unpack(RelationDetailEnvelope.class).getExchangeInput();
        assertThat(expected.getNamesList()).isNotEqualTo(source.getNamesList());
        assertThat(input.getOutputSchema()).isEqualTo(expected);
        assertThat(fixture.producer().stagePlan().requireLiveFragment()).isNotNull();
        assertThat(output.getNamesList()).isEqualTo(fixture.consumer().stagePlan().fieldNames());
    }

    @Test
    void refusesToSerializeConsumerBeforeItsProducerIsPrepared() {
        var fixture = fixture("SELECT 2 AS x");
        assertThatThrownBy(() -> fixture.state().serializePlan(fixture.consumer(), fixture.context()))
                .hasMessageContaining("no dispatched producer plan");
    }

    @Test
    void usesFinalBoundProducerBytesNotTheOriginalFragment() throws Exception {
        var fixture = fixture("SELECT 2 AS x");
        var bound = boundProducer(fixture, false);
        recordDispatchedProducer(fixture, bound.planBytes());
        var output = Plan.parseFrom(fixture.state().serializePlan(fixture.consumer(), fixture.context()).planBytes())
                .getRelations(0).getRoot();
        var input = output.getInput().getExtensionLeaf().getDetail()
                .unpack(RelationDetailEnvelope.class).getExchangeInput();
        assertThat(input.getOutputSchema().getNamesList()).containsExactly("bound_value");
        assertThat(output.getNamesList()).containsExactly("x");
    }

    @Test
    void refusesFinalProducerTypeDriftInsteadOfRenamingItAway() throws Exception {
        var fixture = fixture("SELECT 2 AS x");
        var bound = boundProducer(fixture, true);
        recordDispatchedProducer(fixture, bound.planBytes());
        assertThatThrownBy(() -> fixture.state().serializePlan(fixture.consumer(), fixture.context()))
                .hasMessageContaining("input type differs from producer");
    }

    private static void recordDispatchedProducer(Fixture fixture, byte[] plan) {
        var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "producer-task", "live-wire-query", fixture.producer().stageId(), 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, plan);
        fixture.state().dispatched().add(new org.pangrova.coordinator.query.execution.dispatch.dag
                .DAGDispatchEngine.StageDispatchRecord(fixture.producer().stageId(), null, 0, 0,
                        request));
    }

    private static SerializedPlanFragment boundProducer(Fixture fixture, boolean changeType) throws Exception {
        var serialized = fixture.state().serializePlan(fixture.producer(), fixture.context());
        var plan = Plan.parseFrom(serialized.planBytes());
        var root = plan.getRelations(0).getRoot();
        var schema = SubstraitRelationLayout.describe(root.getInput()).outputSchema().toBuilder()
                .clearNames().addNames("bound_value");
        if (changeType) {
            schema.setStruct(schema.getStruct().toBuilder().setTypes(0, Type.newBuilder()
                    .setString(Type.String.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED))));
        }
        var input = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("actual_bound_source")));
        var exchange = root.getInput().getExchange().toBuilder().setInput(input);
        return new SerializedPlanFragment(plan.toBuilder().setRelations(0, plan.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(Rel.newBuilder().setExchange(exchange))))
                .build().toByteArray(), List.of(), List.of());
    }

    private static Fixture fixture(String sql) {
        var compiler = new SqlCompiler();
        var compiled = compiler.compileForPolicyApplication(sql);
        var node = compiled.plan();
        var exchange = new PangrovaExchange(node.getCluster(), node.getTraitSet(), node,
                PangrovaDistribution.SINGLETON, 1);
        var dag = StageDAG.fromFragmentedPlan(PlannedQuery.forTesting(exchange).fragment(1));
        assertThat(dag.stageEdges()).hasSize(1);
        var edge = dag.stageEdges().get(0);
        var producer = dag.findStageNode(edge.upstreamStageId());
        var consumer = dag.findStageNode(edge.downstreamStageId());
        var session = new CoordinatorSessionContext("live-wire-query", Map.of(),
                new UserIdentity("owner", Set.of(), Map.of(), Instant.EPOCH), "tenant-a");
        var state = DAGExecutionState.create(new StageExecutionContext.Live(session), dag,
                compiler, null, null, ignored -> { });
        var context = DAGQueryContext.readOnly(session);
        return new Fixture(state, context, producer, consumer);
    }

    private record Fixture(DAGExecutionState state, DAGQueryContext context, StageNode producer, StageNode consumer) {}
}
