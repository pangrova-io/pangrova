package org.pangrova.planner.databuild;

import com.google.protobuf.Any;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBuildDetail;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;

import static org.pangrova.protocol.capability.BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN;

/** Produces the deterministic generic Substrait sink used for Native BM25 pages. */
@Internal
public final class Bm25DataBuildPlanBuilder {

    public static final String PLAN_NODE_ID = "artifact-build-bm25";
    public static final int REPORT_SCHEMA_VERSION = 2;

    private Bm25DataBuildPlanBuilder() {}

    /** Builds the native BM25 artifact plan for one text column. */
    public static DataBuildPlan build(List<String> tableName, String textColumn) {
        List<String> names = requireTableName(tableName);
        requireColumnName(textColumn);

        NamedStruct schema = NamedStruct.newBuilder()
                .addNames(PHYSICAL_ROW_POSITION_COLUMN)
                .addNames(textColumn)
                .setStruct(Type.Struct.newBuilder()
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setString(Type.String.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE))))
                .build();
        ReadRel read = ReadRel.newBuilder()
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(names))
                .setBaseSchema(schema)
                .build();
        ArtifactBuildDetail detail = ArtifactBuildDetail.newBuilder()
                .setPlanNodeId(PLAN_NODE_ID)
                .setReportSchemaVersion(REPORT_SCHEMA_VERSION)
                .addInputFields(PHYSICAL_ROW_POSITION_COLUMN)
                .addInputFields(textColumn)
                .setInvocation(CapabilityInvocation.newBuilder()
                        .setCapability(BuiltInCapabilityCatalog.bm25ArtifactBuildPlanRef())
                        .addArguments(fieldArgument("row_position", 0))
                        .addArguments(fieldArgument("text", 1)))
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
        return new DataBuildPlan(
                plan.build().toByteArray(), PLAN_NODE_ID,
                BuiltInCapabilityCatalog.bm25ArtifactBuildPlanRef(),
                BuiltInCapabilityCatalog.bm25ArtifactFormat(),
                BuiltInCapabilityCatalog.physicalRowLocatorContract());
    }

    private static List<String> requireTableName(List<String> tableName) {
        List<String> names = List.copyOf(tableName);
        if (names.isEmpty() || names.stream().anyMatch(value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException("tableName must contain non-blank components");
        }
        return names;
    }

    private static void requireColumnName(String textColumn) {
        if (textColumn == null || textColumn.isBlank()
                || PHYSICAL_ROW_POSITION_COLUMN.equals(textColumn)) {
            throw new IllegalArgumentException("textColumn must be a non-reserved name");
        }
    }

    private static NamedExpressionArgument fieldArgument(String name, int index) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index))
                .build();
    }
}
