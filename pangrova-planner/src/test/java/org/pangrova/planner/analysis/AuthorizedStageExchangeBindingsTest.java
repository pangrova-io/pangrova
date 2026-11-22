package org.pangrova.planner.analysis;

import com.google.protobuf.ByteString;
import org.pangrova.planner.enums.StageTerminalPurpose;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.planner.analysis.CanonicalExchangeGraphFixture.*;

/** Complete immutable graph/schema checks, not a Native result-value or distributed transport oracle. */
class AuthorizedStageExchangeBindingsTest {
    @Test
    void liveExchangeUsesDispatchedProducerWireNamesInsteadOfConsumerAliases() throws Exception {
        var source = schema();
        var aliases = source.toBuilder().setNames(0, "display_record").build();
        var producer = plan(producer(values(source, 3)), aliases.getNamesList());
        var wire = ByteString.copyFrom(producer.planBytes());
        var consumer = plan(consumer(aliases), aliases.getNamesList());
        var bound = AuthorizedStageGraph.bindLiveExchangeInputs(0, consumer, List.of(edge()),
                Map.of(1, List.of(wire, wire, ByteString.copyFrom(producer.planBytes()),
                        ByteString.copyFrom(plan(producer(values(source, 4)), aliases.getNamesList()).planBytes()))));
        var root = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot();
        var detail = root.getInput().getExtensionLeaf().getDetail()
                .unpack(org.pangrova.protocol.v2.RelationDetailEnvelope.class).getExchangeInput();
        assertThat(detail.getOutputSchema()).isEqualTo(source);
        assertThat(root.getNames(0)).isEqualTo("display_record");
        assertThatThrownBy(() -> AuthorizedStageGraph.bindLiveExchangeInputs(0, consumer, List.of(edge()),
                Map.of(1, List.of(wire, ByteString.copyFrom(plan(producer(values(aliases, 3)),
                        aliases.getNamesList()).planBytes())))))
                .hasMessageContaining("different schemas");
        assertThatThrownBy(() -> AuthorizedStageGraph.bindLiveExchangeInputs(0, consumer, List.of(edge()), Map.of()))
                .hasMessageContaining("no dispatched producer");
    }

    @Test
    void matchesCompleteNestedSchemaWithoutUsingPresentationNamesAsWireIdentity() throws Exception {
        NamedStruct schema = schema();
        var producer = stage(1, producer(values(schema, 3)),
                schema.getNamesList().stream().map(name -> "display_" + name).toList());
        var graph = new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(producer, stage(0, consumer(schema), schema.getNamesList())), List.of(edge()));
        var serialized = Plan.parseFrom(graph.stages().get(1).serializedPlan().planBytes());
        assertThat(serialized.getRelations(0).getRoot().getNames(0)).isEqualTo("display_record");
        assertThat(SubstraitRelationLayout.describe(serialized.getRelations(0).getRoot().getInput()).outputSchema())
                .isEqualTo(schema);
        assertThat(serialized.getRelations(0).getRoot().getInput().getExchange().getInput()
                .getRead().getVirtualTable().getValuesCount()).isEqualTo(3);
    }

    @Test
    void rejectsDfsNamesNullabilityPrecisionScaleAndVariationDrift() {
        var original = schema();
        Type row = original.getStruct().getTypes(0);
        Type decimal = row.getStruct().getTypes(0);
        var changedNames = original.toBuilder().setNames(1, "wrong_amount").build();
        var changedRoot = original.toBuilder().setStruct(original.getStruct().toBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        List<Type> decimals = List.of(decimal.toBuilder().setDecimal(decimal.getDecimal().toBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build(),
                decimal.toBuilder().setDecimal(decimal.getDecimal().toBuilder().setPrecision(37)).build(),
                decimal.toBuilder().setDecimal(decimal.getDecimal().toBuilder().setScale(17)).build(),
                decimal.toBuilder().setDecimal(decimal.getDecimal().toBuilder().setTypeVariationReference(13)).build());
        for (NamedStruct changed : List.of(changedNames, changedRoot)) assertMismatch(original, changed);
        for (Type changed : decimals) {
            assertMismatch(original, original.toBuilder().setStruct(original.getStruct().toBuilder().setTypes(0,
                    row.toBuilder().setStruct(row.getStruct().toBuilder().setTypes(0, changed)))).build());
        }
        assertThatThrownBy(() -> graph(original, original.toBuilder().addNames("unused").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unused DFS");
        assertThatThrownBy(() -> graph(original, original.toBuilder().clearNames().addNames("incomplete").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void producerEmitIsTheExactWireOrderAndCannotBeReconstructedFromItsInput() {
        var original = schema();
        Rel source = producer(values(original, 3));
        source = source.toBuilder().setExchange(source.getExchange().toBuilder().setCommon(RelCommon.newBuilder()
                .setEmit(RelCommon.Emit.newBuilder().addOutputMapping(1).addOutputMapping(0)))).build();
        var expected = SubstraitRelationLayout.describe(source).outputSchema();
        var producer = stage(1, source, expected.getNamesList());
        var graph = new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(producer, stage(0, consumer(expected), expected.getNamesList())), List.of(edge()));
        assertThat(graph.stages().get(1).serializedPlan().planBytes()).isEqualTo(producer.serializedPlan().planBytes());
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(producer, stage(0, consumer(original), original.getNamesList())), List.of(edge())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("frozen producer schema");
    }

    @Test
    void zeroColumnThreeRowValuesKeepTheirCardinalityAndEmptyWireSchema() throws Exception {
        var empty = schema().toBuilder().clearNames().setStruct(schema().getStruct().toBuilder().clearTypes()).build();
        var graph = graph(empty, empty);
        var root = Plan.parseFrom(graph.stages().get(1).serializedPlan().planBytes()).getRelations(0).getRoot();
        assertThat(root.getNamesList()).isEmpty();
        assertThat(root.getInput().getExchange().getInput().getRead().getVirtualTable().getValuesCount()).isEqualTo(3);
        assertThat(graph.stages().get(0).outputNames()).isEmpty();
    }

    @Test
    void missingStrayDuplicateAndUnboundProducerInputsFailClosed() {
        var schema = schema();
        var source = stage(1, producer(values(schema, 3)), schema.getNamesList());
        var missing = stage(0, values(schema, 3), schema.getNamesList());
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(source, missing), List.of(edge()))).hasMessageContaining("graph edges");
        assertThatThrownBy(() -> AuthorizedStageGraph.validateExchangeContracts(
                Map.of(0, plan(consumer(schema), schema.getNamesList())), List.of(edge())))
                .hasMessageContaining("both frozen stage plans");
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(stage(0, consumer(schema), schema.getNamesList())), List.of())).hasMessageContaining("graph edges");
        var duplicate = Rel.newBuilder().setCross(io.substrait.proto.CrossRel.newBuilder()
                .setLeft(consumer(schema)).setRight(consumer(schema))).build();
        var names = List.of("left_record", "amount", "created", "left_rows", "sum", "at", "left_lookup", "va", "vc",
                "right_record", "amount", "created", "right_rows", "sum", "at", "right_lookup", "va", "vc");
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(source, stage(0, duplicate, names)), List.of(edge()))).hasMessageContaining("Duplicate exchange input");
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(stage(1, values(schema, 3), schema.getNamesList()),
                        stage(0, consumer(schema), schema.getNamesList())), List.of(edge())))
                .hasMessageContaining("terminal exchange");
    }

    @Test
    void exactProducerDistributionCannotBeSubstitutedBySameCountRoundRobin() {
        var schema = schema();
        Rel source = producer(values(schema, 3));
        source = source.toBuilder().setExchange(source.getExchange().toBuilder()
                .setRoundRobin(source.getExchange().getRoundRobin().toBuilder().setExact(false))).build();
        var changed = stage(1, source, schema.getNamesList());
        assertThatThrownBy(() -> new AuthorizedStageGraph(StageTerminalPurpose.RELATIONAL_EXPORT,
                List.of(changed, stage(0, consumer(schema), schema.getNamesList())), List.of(edge())))
                .hasMessageContaining("Producer distribution differs");
    }

    @Test
    void deterministicSchemaNamePerturbationsNeverBecomeReplayRepair() {
        Random random = new Random(0x4152524f57L);
        var original = schema();
        for (int trial = 0; trial < 64; trial++) {
            int index = random.nextInt(original.getNamesCount());
            var changed = original.toBuilder().setNames(index, original.getNames(index) + "_" + trial).build();
            assertMismatch(original, changed);
        }
    }

    private static void assertMismatch(NamedStruct source, NamedStruct consumer) {
        assertThatThrownBy(() -> graph(source, consumer)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("frozen producer schema");
    }
}
