package org.pangrova.planner.generation;

import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;

import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Bm25GenerationPlanBuilderTest {

    @Test
    void buildsDeterministicCanonicalArtifactSink() throws Exception {
        GenerationPlan first = Bm25GenerationPlanBuilder.build(
                List.of("lake", "db", "items"), "description");
        GenerationPlan second = Bm25GenerationPlanBuilder.build(
                List.of("lake", "db", "items"), "description");

        assertThat(first.planBytes()).isEqualTo(second.planBytes());
        assertThat(first.capability())
                .isEqualTo(BuiltInCapabilityCatalog.bm25ArtifactBuildPlanRef());
        assertThat(first.format()).isEqualTo(BuiltInCapabilityCatalog.bm25ArtifactFormat());
        assertThat(first.rowLocator())
                .isEqualTo(BuiltInCapabilityCatalog.physicalRowLocatorContract());

        Plan plan = Plan.parseFrom(first.planBytes());
        SubstraitProtocolVersion.validate(plan);
        assertThat(plan.getRelations(0).getRoot().getNamesList()).isEmpty();
        var extension = plan.getRelations(0).getRoot().getInput().getExtensionSingle();
        assertThat(extension.getInput().getRead().getNamedTable().getNamesList())
                .containsExactly("lake", "db", "items");
        assertThat(extension.getInput().getRead().getBaseSchema().getNamesList())
                .containsExactly(BuiltInCapabilityIdentifiers.PHYSICAL_ROW_POSITION_COLUMN,
                        "description");
        var types = extension.getInput().getRead().getBaseSchema().getStruct().getTypesList();
        assertThat(types.get(0).getI64().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(types.get(1).getString().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        RelationDetailEnvelope envelope = extension.getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(envelope.getArtifactBuild().getPlanNodeId())
                .isEqualTo(Bm25GenerationPlanBuilder.PLAN_NODE_ID);
        assertThat(envelope.getArtifactBuild().getInvocation().getArgumentsList())
                .extracting(argument -> argument.getName())
                .containsExactly("row_position", "text");
    }

    @Test
    void rejectsReservedOrIncompleteNames() {
        assertThatThrownBy(() -> Bm25GenerationPlanBuilder.build(
                List.of("items"), BuiltInCapabilityIdentifiers.PHYSICAL_ROW_POSITION_COLUMN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Bm25GenerationPlanBuilder.build(List.of(), "description"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
