package org.pangrova.planner.generation;

import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GeneratedValueGenerationPlanBuilderTest {

    @Test
    void compilesModelProjectAndGenericArtifactSinkIntoOneCanonicalPlan() throws Exception {
        var built = GeneratedValueGenerationPlanBuilder.build(
                locatedSource(List.of("title", "body")),
                "embedding",
                List.of("title", "body"),
                "catalog.embed-v3",
                768);

        assertThat(built.modelInvocation().invocationId())
                .isEqualTo(GeneratedValueGenerationPlanBuilder.MODEL_INVOCATION_ID);
        assertThat(built.artifact().capability())
                .isEqualTo(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef());
        assertThat(built.artifact().format())
                .isEqualTo(BuiltInCapabilityCatalog.generatedValuesArtifactFormat());

        Plan plan = Plan.parseFrom(built.artifact().planBytes());
        assertThat(plan.getRelations(0).getRoot().getNamesList()).isEmpty();
        assertThat(plan.getExtensionUrnsList())
                .extracting(io.substrait.proto.SimpleExtensionURN::getUrn)
                .containsExactly(
                        "extension:org.pangrova:functions_ai",
                        "extension:io.substrait:functions_string");
        assertThat(plan.getExtensionsList())
                .extracting(extension -> extension.getExtensionFunction().getName())
                .containsExactly("ai_embed:", "concat:");

        var sink = plan.getRelations(0).getRoot().getInput().getExtensionSingle();
        var project = sink.getInput().getProject();
        assertThat(org.pangrova.protocol.substrait.SubstraitRelationLayout.describe(sink.getInput())
                .outputSchema().getNamesList())
                .containsExactly("$pangrova$row_position", "proj_1");
        assertThat(org.pangrova.protocol.substrait.SubstraitRelationLayout.describe(project.getInput())
                .outputSchema().getNamesList())
                .containsExactly("$pangrova$row_position", "title", "body");
        assertThat(project.getInput().getProject().getInput().getRead().getBaseSchema().getNamesList())
                .containsExactly("title", "body", "$pangrova$row_position");
        assertThat(project.getExpressionsCount()).isEqualTo(2);
        assertThat(project.getExpressions(0).getSelection()
                .getDirectReference().getStructField().getField()).isZero();
        var embedding = project.getExpressions(1).getScalarFunction();
        assertThat(embedding.getFunctionReference()).isEqualTo(1);
        assertThat(embedding.getArgumentsCount()).isEqualTo(4);
        assertThat(embedding.getArguments(0).getValue().getLiteral().getString())
                .isEqualTo("catalog.embed-v3");
        assertThat(embedding.getArguments(1).getValue().getScalarFunction()
                .getFunctionReference()).isEqualTo(2);
        assertOrderedSource(embedding.getArguments(1).getValue().getScalarFunction());
        assertThat(embedding.getArguments(2).getValue().getLiteral().getString())
                .isEqualTo("DOCUMENT");
        assertThat(embedding.getArguments(3).getValue().getLiteral().getString())
                .isEqualTo(GeneratedValueGenerationPlanBuilder.MODEL_INVOCATION_ID);

        RelationDetailEnvelope envelope = sink.getDetail().unpack(
                RelationDetailEnvelope.class);
        assertThat(envelope.getArtifactBuild().getPlanNodeId())
                .isEqualTo(GeneratedValueGenerationPlanBuilder.ARTIFACT_PLAN_NODE_ID);
        assertThat(envelope.getArtifactBuild().getInputFieldsList())
                .containsExactly("$pangrova$row_position", "proj_1");
        assertThat(envelope.getArtifactBuild().getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef());
    }

    @Test
    void emitsDeterministicBytesAndAvoidsConcatForOneSource() throws Exception {
        var first = GeneratedValueGenerationPlanBuilder.build(
                locatedSource(List.of("body")),
                "embedding",
                List.of("body"),
                "catalog.embed-v3",
                768);
        var second = GeneratedValueGenerationPlanBuilder.build(
                locatedSource(List.of("body")),
                "embedding",
                List.of("body"),
                "catalog.embed-v3",
                768);

        assertThat(second.artifact().planBytes())
                .containsExactly(first.artifact().planBytes());
        Plan plan = Plan.parseFrom(first.artifact().planBytes());
        assertThat(plan.getExtensionUrnsList()).hasSize(1);
        var input = plan.getRelations(0).getRoot().getInput()
                .getExtensionSingle().getInput().getProject()
                .getExpressions(1).getScalarFunction()
                .getArguments(1).getValue();
        assertThat(input.hasSelection()).isTrue();
        assertThat(input.getSelection().getDirectReference()
                .getStructField().getField()).isOne();
    }

    private static void assertOrderedSource(io.substrait.proto.Expression.ScalarFunction source) {
        assertThat(source.getArgumentsCount()).isEqualTo(3);
        assertThat(source.getArguments(0).getValue().getSelection().getDirectReference()
                .getStructField().getField()).isOne();
        assertThat(source.getArguments(1).getValue().getLiteral().getString())
                .isEqualTo(org.pangrova.planner.embedding.GeneratedColumnSourceEncoding.SEPARATOR);
        assertThat(source.getArguments(2).getValue().getSelection().getDirectReference()
                .getStructField().getField()).isEqualTo(2);
    }

    private static byte[] locatedSource(List<String> columns) {
        var types = io.substrait.proto.Type.Struct.newBuilder();
        columns.forEach(column -> types.addTypes(io.substrait.proto.Type.newBuilder().setString(
                io.substrait.proto.Type.String.newBuilder()
                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))));
        var schema = io.substrait.proto.NamedStruct.newBuilder().addAllNames(columns).setStruct(types);
        var read = io.substrait.proto.ReadRel.newBuilder().setBaseSchema(schema).setNamedTable(
                io.substrait.proto.ReadRel.NamedTable.newBuilder().addAllNames(List.of("lake", "sales", "documents")));
        var plan = Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder().setRoot(
                io.substrait.proto.RelRoot.newBuilder().setInput(io.substrait.proto.Rel.newBuilder().setRead(read))
                        .addAllNames(columns)));
        org.pangrova.protocol.substrait.SubstraitProtocolVersion.stamp(plan);
        return PhysicalRowLocatorPlanAugmenter.prepend(plan.build().toByteArray(),
                List.of("lake", "sales", "documents"));
    }
}
