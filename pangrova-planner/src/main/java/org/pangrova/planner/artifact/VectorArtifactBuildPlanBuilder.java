package org.pangrova.planner.artifact;

import com.google.protobuf.Any;
import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBuildDetail;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.spi.annotation.Internal;

import java.util.List;

import static org.pangrova.protocol.BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN;

/** Produces the one canonical Substrait artifact sink for every Native ANN algorithm. */
@Internal
public final class VectorArtifactBuildPlanBuilder {

    public static final int REPORT_SCHEMA_VERSION = 2;

    private VectorArtifactBuildPlanBuilder() {}

    /** Builds a deterministic Native artifact relation for the selected ANN capability. */
    public static ArtifactBuildPlan build(
            List<String> tableName, String vectorColumn,
            CapabilityRef buildCapability, ArtifactFormatRef artifactFormat) {
        List<String> names = requireInputs(tableName, vectorColumn);
        validateCanonicalReferences(buildCapability, artifactFormat);
        String capabilityId = buildCapability.getCapabilityId();
        String planNodeId = "artifact-build-"
                + capabilityId.substring(capabilityId.lastIndexOf('.') + 1)
                        .replace('_', '-');

        NamedStruct schema = NamedStruct.newBuilder()
                .addNames(PHYSICAL_ROW_POSITION_COLUMN)
                .addNames(vectorColumn)
                .setStruct(Type.Struct.newBuilder()
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setList(Type.List.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
                                .setType(Type.newBuilder().setFp32(Type.FP32.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED))))))
                .build();
        ReadRel read = ReadRel.newBuilder()
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(names))
                .setBaseSchema(schema)
                .build();
        ArtifactBuildDetail detail = ArtifactBuildDetail.newBuilder()
                .setPlanNodeId(planNodeId)
                .setReportSchemaVersion(REPORT_SCHEMA_VERSION)
                .addInputFields(PHYSICAL_ROW_POSITION_COLUMN)
                .addInputFields(vectorColumn)
                .setInvocation(CapabilityInvocation.newBuilder()
                        .setCapability(buildCapability)
                        .addArguments(fieldArgument("row_position", 0))
                        .addArguments(fieldArgument("vector", 1)))
                .build();
        RelationDetailEnvelope envelope = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setArtifactBuild(detail)
                .build();
        Rel relation = Rel.newBuilder()
                .setExtensionSingle(io.substrait.proto.ExtensionSingleRel.newBuilder()
                        .setInput(Rel.newBuilder().setRead(read))
                        .setDetail(Any.pack(envelope)))
                .build();
        Plan.Builder plan = Plan.newBuilder()
                .addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                        .setInput(relation)));
        SubstraitProtocolVersion.stamp(plan);
        org.pangrova.protocol.substrait.SubstraitRelationLayout.rootSchema(plan.getRelations(0).getRoot());
        return new ArtifactBuildPlan(
                plan.build().toByteArray(), planNodeId, buildCapability,
                artifactFormat,
                BuiltInCapabilityCatalog.physicalRowLocatorContract());
    }

    private static List<String> requireInputs(
            List<String> tableName, String vectorColumn) {
        List<String> names = List.copyOf(tableName);
        if (names.isEmpty() || names.stream().anyMatch(
                value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException("tableName must contain non-blank components");
        }
        if (vectorColumn == null || vectorColumn.isBlank()
                || PHYSICAL_ROW_POSITION_COLUMN.equals(vectorColumn)) {
            throw new IllegalArgumentException("vectorColumn must be a non-reserved name");
        }
        return names;
    }

    private static void validateCanonicalReferences(
            CapabilityRef capability, ArtifactFormatRef format) {
        if (capability == null
                || capability.getCapabilityId().isBlank()
                || capability.getSemanticVersion().isBlank()
                || capability.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "buildCapability must be a complete canonical reference");
        }
        if (format == null
                || format.getFormatId().isBlank()
                || format.getMajorVersion() == 0
                || format.getSchemaSha256().size() != 32
                || format.getWriterBuildId().isBlank()) {
            throw new IllegalArgumentException(
                    "artifactFormat must be a complete canonical reference");
        }
    }

    private static NamedExpressionArgument fieldArgument(String name, int index) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index))
                .build();
    }
}
