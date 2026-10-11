package org.pangrova.planner.artifact;

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.spi.annotation.Internal;

import java.util.List;
import java.util.Objects;

import static org.pangrova.protocol.BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN;

/** Carries the storage row locator through an already authorized streaming map. */
@Internal
public final class PhysicalRowLocatorPlanAugmenter {

    private PhysicalRowLocatorPlanAugmenter() {}

    /**
     * Preserves the authorized expressions and their ordinals, then prepends the
     * physical locator to the root output. This transformation grants no access;
     * callers must supply the server-compiled protected plan for the exact source.
     */
    public static byte[] prepend(byte[] authorizedPlanBytes, List<String> exactSourceTable) {
        Objects.requireNonNull(authorizedPlanBytes, "authorizedPlanBytes");
        List<String> source = List.copyOf(exactSourceTable);
        if (source.isEmpty() || source.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("An exact source table is required");
        }
        Plan plan = parse(authorizedPlanBytes);
        SubstraitProtocolVersion.validate(plan);
        if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException("A single authorized root is required");
        }
        var root = plan.getRelations(0).getRoot();
        Augmented augmented = augment(root.getInput(), source);
        var output = SubstraitRelationLayout.describe(root.getInput()).outputSchema();
        if (root.getNamesCount() != output.getNamesCount()
                || root.getNamesList().contains(PHYSICAL_ROW_POSITION_COLUMN)) {
            throw new IllegalArgumentException("Authorized root output schema is invalid");
        }
        RelCommon.Emit.Builder order = RelCommon.Emit.newBuilder()
                .addOutputMapping(augmented.originalFields());
        for (int index = 0; index < augmented.originalFields(); index++) {
            order.addOutputMapping(index);
        }
        Rel reordered = Rel.newBuilder().setProject(ProjectRel.newBuilder()
                .setInput(augmented.relation())
                .setCommon(RelCommon.newBuilder().setEmit(order))).build();
        var newRoot = root.toBuilder().setInput(reordered).clearNames()
                .addNames(PHYSICAL_ROW_POSITION_COLUMN).addAllNames(root.getNamesList());
        return plan.toBuilder().setRelations(0,
                plan.getRelations(0).toBuilder().setRoot(newRoot)).build().toByteArray();
    }

    private static Plan parse(byte[] bytes) {
        try {
            return Plan.parseFrom(bytes);
        } catch (InvalidProtocolBufferException exception) {
            throw new IllegalArgumentException("Authorized source plan is malformed", exception);
        }
    }

    private static Augmented augment(Rel relation, List<String> source) {
        return switch (relation.getRelTypeCase()) {
            case READ -> augmentRead(relation, source);
            case FILTER -> augmentFilter(relation, source);
            case PROJECT -> augmentProject(relation, source);
            default -> throw new IllegalArgumentException(
                    "Physical row locators require a linear Read/Filter/Project plan");
        };
    }

    private static Augmented augmentRead(Rel relation, List<String> source) {
        ReadRel read = relation.getRead();
        if (!read.hasNamedTable() || !read.getNamedTable().getNamesList().equals(source)
                || !read.hasBaseSchema() || read.hasProjection()
                || read.getBaseSchema().getNamesList().contains(PHYSICAL_ROW_POSITION_COLUMN)) {
            throw new IllegalArgumentException("Authorized read must name the exact physical source");
        }
        int inputFields = read.getBaseSchema().getStruct().getTypesCount();
        int outputFields = outputCount(read.getCommon(), inputFields);
        Type position = Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        var schema = read.getBaseSchema().toBuilder()
                .addNames(PHYSICAL_ROW_POSITION_COLUMN)
                .setStruct(read.getBaseSchema().getStruct().toBuilder().addTypes(position));
        Rel result = relation.toBuilder().setRead(read.toBuilder().setBaseSchema(schema)
                .setCommon(SubstraitRelationLayout.withAppendedInput(
                        read.getCommon(), inputFields, 0, 1, true))).build();
        return new Augmented(result, outputFields);
    }

    private static Augmented augmentFilter(Rel relation, List<String> source) {
        var filter = relation.getFilter();
        Augmented input = augment(filter.getInput(), source);
        int outputFields = outputCount(filter.getCommon(), input.originalFields());
        Rel result = relation.toBuilder().setFilter(filter.toBuilder().setInput(input.relation())
                .setCommon(SubstraitRelationLayout.withAppendedInput(filter.getCommon(),
                        input.originalFields(), 0, 1, true))).build();
        return new Augmented(result, outputFields);
    }

    private static Augmented augmentProject(Rel relation, List<String> source) {
        var project = relation.getProject();
        Augmented input = augment(project.getInput(), source);
        int directFields = Math.addExact(input.originalFields(), project.getExpressionsCount());
        int outputFields = outputCount(project.getCommon(), directFields);
        Rel result = relation.toBuilder().setProject(project.toBuilder().setInput(input.relation())
                .setCommon(SubstraitRelationLayout.withAppendedInput(project.getCommon(), input.originalFields(),
                        project.getExpressionsCount(), 1, true))).build();
        return new Augmented(result, outputFields);
    }

    private static int outputCount(RelCommon common, int directFields) {
        return SubstraitRelationLayout.outputMapping(common, directFields).size();
    }

    private record Augmented(Rel relation, int originalFields) {
    }
}
