package org.pangrova.planner.artifact;

import com.google.protobuf.Any;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBuildDetail;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.spi.annotation.Internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.pangrova.protocol.BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN;

/** Builds one deterministic Native source-map plan for a generated VECTOR value. */
@Internal
public final class GeneratedValueArtifactBuildPlanBuilder {

    public static final String ARTIFACT_PLAN_NODE_ID =
            "artifact-build-generated-values";
    public static final String MODEL_INVOCATION_ID =
            "generated-value-model-invocation";
    public static final int REPORT_SCHEMA_VERSION = 2;
    public static final String SOURCE_ENCODING = org.pangrova.planner.embedding.GeneratedColumnSourceEncoding.NAME;

    private static final String SOURCE_SEPARATOR =
            org.pangrova.planner.embedding.GeneratedColumnSourceEncoding.SEPARATOR;

    private GeneratedValueArtifactBuildPlanBuilder() {}

    /** Appends model/project/sink to a server-authorized source with its physical locator. */
    public static GeneratedValueArtifactBuildPlan build(
            byte[] locatedSourcePlan, String targetColumn, List<String> sourceColumns,
            String logicalModelName, int dimension) {
        List<String> sources = requireColumns(targetColumn, sourceColumns);
        requireText(logicalModelName, "logicalModelName");
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        Plan source = requireSource(locatedSourcePlan, sources);
        Anchors anchors = nextAnchors(source);
        Rel projected = projectedSource(source, sources.size(), logicalModelName, anchors);
        Plan.Builder plan = source.toBuilder().clearRelations()
                .addExtensionUrns(extensionUrn(anchors.aiUrn(), "extension:org.pangrova:functions_ai"))
                .addExtensions(extensionFunction(anchors.aiUrn(), anchors.embedFunction(), "ai_embed:"))
                .addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                        .setInput(artifactSink(projected, targetColumn))));
        if (sources.size() > 1) {
            plan.addExtensionUrns(extensionUrn(anchors.stringUrn(), "extension:io.substrait:functions_string"))
                    .addExtensions(extensionFunction(anchors.stringUrn(), anchors.concatFunction(), "concat:"));
        }
        org.pangrova.protocol.substrait.SubstraitRelationLayout.rootSchema(plan.getRelations(0).getRoot());
        return new GeneratedValueArtifactBuildPlan(new ArtifactBuildPlan(plan.build().toByteArray(),
                ARTIFACT_PLAN_NODE_ID, BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef(),
                BuiltInCapabilityCatalog.generatedValuesArtifactFormat(),
                BuiltInCapabilityCatalog.physicalRowLocatorContract()),
                new AiInvocationPlanHint(MODEL_INVOCATION_ID, "AI_EMBED", logicalModelName,
                        AiInvocationPlanHint.Operation.EMBEDDING, false));
    }

    private static Plan requireSource(byte[] bytes, List<String> sources) {
        try {
            Plan plan = Plan.parseFrom(bytes);
            SubstraitProtocolVersion.validate(plan);
            var expectedNames = new ArrayList<String>();
            expectedNames.add(PHYSICAL_ROW_POSITION_COLUMN);
            expectedNames.addAll(sources);
            if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()
                    || !plan.getRelations(0).getRoot().getNamesList().equals(expectedNames)) {
                throw new IllegalArgumentException("Generated source must preserve exact authorized column order");
            }
            return plan;
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed authorized generated source", malformed);
        }
    }

    private static Anchors nextAnchors(Plan source) {
        int urn = source.getExtensionUrnsList().stream()
                .mapToInt(SimpleExtensionURN::getExtensionUrnAnchor).max().orElse(0);
        int function = source.getExtensionsList().stream().filter(SimpleExtensionDeclaration::hasExtensionFunction)
                .mapToInt(extension -> extension.getExtensionFunction().getFunctionAnchor()).max().orElse(0);
        if (urn < 0 || function < 0 || urn > Integer.MAX_VALUE - 2 || function > Integer.MAX_VALUE - 2) {
            throw new IllegalArgumentException("Authorized source extension anchor space is exhausted");
        }
        return new Anchors(urn + 1, urn + 2, function + 1, function + 2);
    }

    private static Rel projectedSource(Plan source, int sourceCount, String model, Anchors anchors) {
        int inputFields = sourceCount + 1;
        return Rel.newBuilder().setProject(ProjectRel.newBuilder()
                .setInput(source.getRelations(0).getRoot().getInput())
                .addExpressions(field(0))
                .addExpressions(embeddingExpression(model, sourceExpression(sourceCount, anchors.concatFunction()),
                        anchors.embedFunction()))
                .setCommon(io.substrait.proto.RelCommon.newBuilder().setEmit(
                        io.substrait.proto.RelCommon.Emit.newBuilder()
                                .addOutputMapping(inputFields).addOutputMapping(inputFields + 1))))
                .build();
    }

    private static Rel artifactSink(Rel projected, String targetColumn) {
        ArtifactBuildDetail detail = ArtifactBuildDetail.newBuilder()
                .setPlanNodeId(ARTIFACT_PLAN_NODE_ID).setReportSchemaVersion(REPORT_SCHEMA_VERSION)
                .addInputFields(PHYSICAL_ROW_POSITION_COLUMN).addInputFields(targetColumn)
                .setInvocation(CapabilityInvocation.newBuilder()
                        .setCapability(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef())
                        .addArguments(fieldArgument("row_position", 0))
                        .addArguments(fieldArgument("generated_value", 1)))
                .build();
        var envelope = RelationDetailEnvelope.newBuilder().setSchemaVersion(1).setArtifactBuild(detail).build();
        return Rel.newBuilder().setExtensionSingle(io.substrait.proto.ExtensionSingleRel.newBuilder()
                .setInput(projected).setDetail(Any.pack(envelope))).build();
    }

    private record Anchors(int aiUrn, int stringUrn, int embedFunction, int concatFunction) {}

    private static Expression embeddingExpression(
            String logicalModelName, Expression input, int embedAnchor) {
        Type vectorType = Type.newBuilder().setList(Type.List.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
                .setType(Type.newBuilder().setFp32(Type.FP32.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE))))
                .build();
        return scalar(
                embedAnchor,
                vectorType,
                List.of(
                        stringLiteral(logicalModelName),
                        input,
                        stringLiteral(org.pangrova.spi.ai.embedding.EmbeddingPurpose.DOCUMENT.name()),
                        stringLiteral(MODEL_INVOCATION_ID)));
    }

    private static Expression sourceExpression(int sourceCount, int concatAnchor) {
        if (sourceCount == 1) {
            return field(1);
        }
        List<Expression> arguments = new ArrayList<>(sourceCount * 2 - 1);
        for (int index = 0; index < sourceCount; index++) {
            if (index > 0) {
                arguments.add(stringLiteral(SOURCE_SEPARATOR));
            }
            arguments.add(field(index + 1));
        }
        Type output = Type.newBuilder().setString(Type.String.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        return scalar(concatAnchor, output, arguments);
    }

    private static Expression scalar(
            int functionReference, Type outputType, List<Expression> arguments) {
        Expression.ScalarFunction.Builder function = Expression.ScalarFunction.newBuilder()
                .setFunctionReference(functionReference)
                .setOutputType(outputType);
        arguments.forEach(argument -> function.addArguments(
                FunctionArgument.newBuilder().setValue(argument)));
        return Expression.newBuilder().setScalarFunction(function).build();
    }

    private static Expression field(int index) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index);
    }

    private static Expression stringLiteral(String value) {
        return Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setString(value)
                        .setNullable(false))
                .build();
    }

    private static NamedExpressionArgument fieldArgument(String name, int index) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(field(index))
                .build();
    }

    private static SimpleExtensionURN extensionUrn(int anchor, String urn) {
        return SimpleExtensionURN.newBuilder()
                .setExtensionUrnAnchor(anchor)
                .setUrn(urn)
                .build();
    }

    private static SimpleExtensionDeclaration extensionFunction(
            int urnAnchor, int functionAnchor, String name) {
        return SimpleExtensionDeclaration.newBuilder()
                .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                        .setExtensionUrnReference(urnAnchor)
                        .setFunctionAnchor(functionAnchor)
                        .setName(name))
                .build();
    }

    private static List<String> requireColumns(
            String targetColumn, List<String> sourceColumns) {
        requireColumn(targetColumn, "targetColumn");
        List<String> sources = List.copyOf(sourceColumns);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("sourceColumns must not be empty");
        }
        Set<String> unique = new HashSet<>();
        for (String source : sources) {
            requireColumn(source, "sourceColumn");
            String normalized = source.toLowerCase(Locale.ROOT);
            if (normalized.equals(targetColumn.toLowerCase(Locale.ROOT))
                    || !unique.add(normalized)) {
                throw new IllegalArgumentException(
                        "source columns must be unique and must not include the target");
            }
        }
        return sources;
    }

    private static void requireColumn(String value, String name) {
        requireText(value, name);
        if (PHYSICAL_ROW_POSITION_COLUMN.equals(value)) {
            throw new IllegalArgumentException(name + " must not use a reserved name");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
