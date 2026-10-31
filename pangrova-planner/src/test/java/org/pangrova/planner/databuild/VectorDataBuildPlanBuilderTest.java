package org.pangrova.planner.databuild;

import io.substrait.proto.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorDataBuildPlanBuilderTest {

    @ParameterizedTest
    @MethodSource("nativeArtifactContracts")
    void buildsDeterministicCanonicalArtifactSink(
            CapabilityRef buildCapability, ArtifactFormatRef format) throws Exception {
        DataBuildPlan first = VectorDataBuildPlanBuilder.build(
                List.of("lake", "db", "items"), "embedding",
                buildCapability, format);
        DataBuildPlan second = VectorDataBuildPlanBuilder.build(
                List.of("lake", "db", "items"), "embedding",
                buildCapability, format);

        assertThat(first.planBytes()).isEqualTo(second.planBytes());
        assertThat(first.capability()).isEqualTo(buildCapability);
        assertThat(first.format()).isEqualTo(format);
        assertThat(first.rowLocator())
                .isEqualTo(BuiltInCapabilityCatalog.physicalRowLocatorContract());
        Plan plan = Plan.parseFrom(first.planBytes());
        SubstraitProtocolVersion.validate(plan);
        assertThat(plan.getRelations(0).getRoot().getNamesList()).isEmpty();
        var extension = plan.getRelations(0).getRoot().getInput().getExtensionSingle();
        assertThat(extension.getInput().getRead().getBaseSchema().getNamesList())
                .containsExactly(BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN, "embedding");
        var types = extension.getInput().getRead().getBaseSchema().getStruct().getTypesList();
        assertThat(types.get(0).getI64().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(types.get(1).getList().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(types.get(1).getList().getType().getFp32().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(extension.getInput().getRead().getNamedTable().getNamesList())
                .containsExactly("lake", "db", "items");
        RelationDetailEnvelope envelope = extension.getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(envelope.getArtifactBuild().getPlanNodeId())
                .isEqualTo(first.planNodeId());
        assertThat(envelope.getArtifactBuild().getInvocation().getArgumentsList())
                .extracting(argument -> argument.getName())
                .containsExactly("row_position", "vector");
    }

    @ParameterizedTest
    @MethodSource("nativeArtifactContracts")
    void rejectsReservedOrIncompleteNames(
            CapabilityRef buildCapability, ArtifactFormatRef format) {
        assertThatThrownBy(() -> VectorDataBuildPlanBuilder.build(
                List.of("items"), BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN,
                buildCapability, format)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VectorDataBuildPlanBuilder.build(
                List.of(), "embedding", buildCapability, format))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Stream<Arguments> nativeArtifactContracts() {
        return Stream.of(
                Arguments.of(
                        BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(),
                        BuiltInCapabilityCatalog.hnswArtifactFormat()),
                Arguments.of(
                        BuiltInCapabilityCatalog.ivfPqArtifactBuildPlanRef(),
                        BuiltInCapabilityCatalog.ivfPqArtifactFormat()),
                Arguments.of(
                        BuiltInCapabilityCatalog.diskAnnArtifactBuildPlanRef(),
                        BuiltInCapabilityCatalog.diskAnnArtifactFormat()));
    }
}
