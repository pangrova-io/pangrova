package org.pangrova.protocol.substrait;

import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated-protocol layout oracles, not a substitute for Native Task execution. */
class SubstraitRelationLayoutTest {
    @Test
    void projectDirectAppendsAndEmitPreservesDuplicatesAndExplicitEmptyOutput() {
        Rel source = read(flatSchema());
        Rel project = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(source)
                .addExpressions(integer(17))).build();
        var layout = SubstraitRelationLayout.describe(project);
        assertThat(layout.expressionSchema()).isEqualTo(flatSchema());
        assertThat(layout.directSchema().getStruct().getTypesCount()).isEqualTo(3);
        assertThat(layout.outputMapping()).containsExactly(0, 1, 2);
        Rel repeated = project.toBuilder().setProject(project.getProject().toBuilder()
                .setCommon(emit(2, 0, 2))).build();
        assertThat(SubstraitRelationLayout.topLevelNames(SubstraitRelationLayout.describe(repeated).outputSchema()))
                .containsExactly("proj_0", "id", "proj_0");
        Rel empty = project.toBuilder().setProject(project.getProject().toBuilder().setCommon(emit())).build();
        assertThat(SubstraitRelationLayout.describe(empty).outputSchema().getStruct().getTypesList()).isEmpty();
        assertThat(SubstraitRelationLayout.describe(empty).outputSchema().getNamesList()).isEmpty();
    }

    @Test
    void readFilterUsesBaseSchemaBeforeItsPermissionProjection() {
        var condition = field(1);
        Rel source = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(flatSchema())
                .setFilter(condition).setCommon(emit(0))).build();
        var layout = SubstraitRelationLayout.describe(source);
        assertThat(layout.expressionSchema()).isEqualTo(flatSchema());
        assertThat(SubstraitRelationLayout.topLevelNames(layout.outputSchema())).containsExactly("id");
        Rel filter = Rel.newBuilder().setFilter(FilterRel.newBuilder().setInput(source)
                .setCondition(field(0))).build();
        assertThatThrownBy(() -> SubstraitRelationLayout.describe(filter))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("BOOLEAN");
        Rel invalidRead = source.toBuilder().setRead(source.getRead().toBuilder().setFilter(field(0))).build();
        assertThatThrownBy(() -> SubstraitRelationLayout.describe(invalidRead))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("BOOLEAN");
    }

    @Test
    void dfsNamesRemainAttachedToStructListAndMapKeyValueSubtrees() {
        Type struct = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(i64()).addTypes(bool())).build();
        Type list = Type.newBuilder().setList(Type.List.newBuilder().setType(struct)).build();
        Type map = Type.newBuilder().setMap(Type.Map.newBuilder().setKey(struct).setValue(list)).build();
        var schema = NamedStruct.newBuilder().addAllNames(List.of("record", "a", "b", "rows", "c", "d",
                "lookup", "key_a", "key_b", "value_a", "value_b"))
                .setStruct(Type.Struct.newBuilder().addTypes(struct).addTypes(list).addTypes(map)).build();
        assertThat(SubstraitRelationLayout.topLevelNames(schema)).containsExactly("record", "rows", "lookup");
        var projected = SubstraitRelationLayout.projectSchema(schema, List.of(2, 0, 2));
        assertThat(projected.getNamesList()).containsExactly("lookup", "key_a", "key_b", "value_a", "value_b",
                "record", "a", "b", "lookup", "key_a", "key_b", "value_a", "value_b");
        assertThat(projected.getStruct().getTypesList()).containsExactly(map, struct, map);
        assertThatThrownBy(() -> SubstraitRelationLayout.topLevelNames(schema.toBuilder().addNames("extra").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unused DFS");
    }

    @Test
    void fragmentIndexUsesOriginalInstancesAndCannotSilentlyDescribeDifferentNodes() {
        Rel source = read(flatSchema());
        Rel root = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(source).setCommon(emit(0))).build();
        var tree = SubstraitRelationLayout.describeTree(root);
        assertThat(tree.layout(root.getProject().getInput()).outputSchema()).isEqualTo(flatSchema());
        assertThat(tree.layout(root).outputMapping()).containsExactly(0);
        assertThatThrownBy(() -> tree.layout(read(flatSchema())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("indexed fragment");
    }

    @Test
    void unaryContextsAndPassThroughOutputsPreserveEntireStructMetadata() {
        var schema = flatSchema().toBuilder().setStruct(flatSchema().getStruct().toBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setTypeVariationReference(37)).build();
        Rel source = read(schema);
        Rel project = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(source)).build();
        Rel filter = Rel.newBuilder().setFilter(FilterRel.newBuilder().setInput(project).setCondition(field(1))).build();
        var tree = SubstraitRelationLayout.describeTree(filter);
        assertThat(tree.layout(project).expressionSchema()).isEqualTo(schema);
        assertThat(tree.layout(project).directSchema()).isEqualTo(schema);
        assertThat(tree.layout(project).outputSchema()).isEqualTo(schema);
        assertThat(tree.layout(filter).expressionSchema()).isEqualTo(schema);
        assertThat(tree.layout(filter).outputSchema()).isEqualTo(schema);
    }

    @Test
    void declaredComputedAppendPreservesOriginalDfsAndRootMetadata() {
        var input = flatSchema().toBuilder().setStruct(flatSchema().getStruct().toBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setTypeVariationReference(37)).build();
        Type row = Type.newBuilder().setStruct(Type.Struct.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).addTypes(i64()).addTypes(bool())).build();
        Type array = Type.newBuilder().setList(Type.List.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setType(row)).build();
        var result = SubstraitRelationLayout.appendComputedFields(input, List.of("computed"), List.of(array));
        assertThat(result.getNamesList()).containsExactly("id", "allowed", "computed", "field_0", "field_1");
        assertThat(result.getStruct()).isEqualTo(input.getStruct().toBuilder().addTypes(array).build());
        assertThatThrownBy(() -> SubstraitRelationLayout.appendComputedFields(input, List.of(), List.of(array)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("arity");
    }

    @Test
    void groupingSetsAppendRequiredInt32SetIndexAndNullableUnionKeysBeforeEmit() {
        Rel source = read(flatSchema());
        var aggregate = AggregateRel.newBuilder().setInput(source).addGroupingExpressions(field(0))
                .addGroupings(AggregateRel.Grouping.newBuilder().addExpressionReferences(0))
                .addGroupings(AggregateRel.Grouping.newBuilder())
                .addMeasures(AggregateRel.Measure.newBuilder().setMeasure(io.substrait.proto.AggregateFunction.newBuilder()
                        .setOutputType(i64())));
        var layout = SubstraitRelationLayout.describe(Rel.newBuilder().setAggregate(aggregate).build());
        assertThat(SubstraitRelationLayout.topLevelNames(layout.outputSchema()))
                .containsExactly("id", "measure_0", "grouping_set_index");
        assertThat(layout.outputSchema().getStruct().getTypes(0).getI64().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(layout.outputSchema().getStruct().getTypes(2).getI32().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(SubstraitRelationLayout.describe(Rel.newBuilder().setAggregate(aggregate.setCommon(emit(1))).build())
                .outputSchema().getStruct().getTypesList()).containsExactly(i64());
    }

    @Test
    void groupingOutputUsesDeclarationOrderNotFirstEncounteredSetReferences() {
        var aggregate = AggregateRel.newBuilder().setInput(read(flatSchema()))
                .addGroupingExpressions(field(0)).addGroupingExpressions(field(1))
                .addGroupings(AggregateRel.Grouping.newBuilder().addExpressionReferences(1).addExpressionReferences(0))
                .addGroupings(AggregateRel.Grouping.newBuilder().addExpressionReferences(1));
        var layout = SubstraitRelationLayout.describe(Rel.newBuilder().setAggregate(aggregate).build());
        assertThat(SubstraitRelationLayout.topLevelNames(layout.outputSchema()))
                .containsExactly("id", "allowed", "grouping_set_index");
        assertThat(layout.outputSchema().getStruct().getTypes(0).getI64().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(layout.outputSchema().getStruct().getTypes(1)).isEqualTo(flatSchema().getStruct().getTypes(1));
    }

    @Test
    void setNullabilityDistinguishesUnionAnySecondaryAndAllSecondaryContracts() {
        var required = flatSchema();
        var nullable = required.toBuilder().setStruct(required.getStruct().toBuilder()
                .setTypes(0, i64().toBuilder().setI64(i64().getI64().toBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE)))).build();
        for (var operation : List.of(io.substrait.proto.SetRel.SetOp.SET_OP_UNION_ALL,
                io.substrait.proto.SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY,
                io.substrait.proto.SetRel.SetOp.SET_OP_INTERSECTION_MULTISET)) {
            var set = io.substrait.proto.SetRel.newBuilder().setOp(operation)
                    .addInputs(read(nullable)).addInputs(read(required)).addInputs(read(nullable));
            var output = SubstraitRelationLayout.describe(Rel.newBuilder().setSet(set).build()).outputSchema();
            assertThat(output.getStruct().getTypes(0).getI64().getNullability())
                    .isEqualTo(operation == io.substrait.proto.SetRel.SetOp.SET_OP_INTERSECTION_MULTISET
                            ? Type.Nullability.NULLABILITY_REQUIRED : Type.Nullability.NULLABILITY_NULLABLE);
            assertThat(output.getNamesList()).isEqualTo(nullable.getNamesList());
            assertThat(output.getStruct().getTypeVariationReference()).isEqualTo(nullable.getStruct().getTypeVariationReference());
        }
    }

    @Test
    void decimalTypedNullAndLiteralVariationsAreNeverReinferredFromValues() {
        var decimal = Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(30).setScale(7)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setTypeVariationReference(19)).build();
        var nil = Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setNull(decimal)).build();
        assertThat(SubstraitRelationLayout.typeOf(nil, flatSchema())).isEqualTo(decimal);
        var value = Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(3)
                .setTypeVariationReference(11)).build();
        assertThat(SubstraitRelationLayout.typeOf(value, flatSchema()).getI64().getTypeVariationReference()).isEqualTo(11);
        var bad = Expression.newBuilder().setCast(Expression.Cast.newBuilder().setInput(value).setType(
                decimal.toBuilder().setDecimal(decimal.getDecimal().toBuilder().setScale(31)))).build();
        assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(bad, flatSchema()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision/scale");
    }

    @Test
    void invalidOrMissingSchemasAndEmitsAreRejectedWithoutGuessedColumns() {
        assertThatThrownBy(() -> SubstraitRelationLayout.describe(Rel.newBuilder().setRead(ReadRel.newBuilder()).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit base schema");
        for (int index : List.of(-1, 2)) {
            Rel source = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(flatSchema()).setCommon(emit(index))).build();
            assertThatThrownBy(() -> SubstraitRelationLayout.describe(source))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("out of bounds");
        }
        var empty = NamedStruct.newBuilder().setStruct(Type.Struct.newBuilder()).build();
        assertThat(SubstraitRelationLayout.describe(read(empty)).outputSchema().getStruct().getTypesList()).isEmpty();
    }

    @Test
    void readMaskRetainsRootRowWhileNestedSingularSelectionFollowsItsExplicitFlag() {
        var child = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(i64()).addTypes(bool())
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setTypeVariationReference(23)).build();
        var schema = NamedStruct.newBuilder().addAllNames(List.of("record", "id", "allowed"))
                .setStruct(Type.Struct.newBuilder().addTypes(child)).build();
        var nestedSelect = Expression.MaskExpression.StructSelect.newBuilder().addStructItems(
                Expression.MaskExpression.StructItem.newBuilder().setField(1));
        var mask = Expression.MaskExpression.newBuilder().setSelect(Expression.MaskExpression.StructSelect.newBuilder()
                .addStructItems(Expression.MaskExpression.StructItem.newBuilder().setField(0).setChild(
                        Expression.MaskExpression.Select.newBuilder().setStruct(nestedSelect))));
        Rel source = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema).setProjection(mask)).build();
        var unwrapped = SubstraitRelationLayout.describe(source).outputSchema();
        assertThat(unwrapped.getNamesList()).containsExactly("record");
        assertThat(unwrapped.getStruct().getTypes(0).getBool().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        Rel retained = source.toBuilder().setRead(source.getRead().toBuilder()
                .setProjection(mask.setMaintainSingularStruct(true))).build();
        var output = SubstraitRelationLayout.describe(retained).outputSchema();
        assertThat(output.getNamesList()).containsExactly("record", "allowed");
        assertThat(output.getStruct().getTypes(0).getStruct().getTypeVariationReference()).isEqualTo(23);
        assertThat(output.getStruct().getTypes(0).getStruct().getTypesList()).containsExactly(bool());
    }

    @Test
    void physicalAliasesNeverOverwriteLogicalDuplicateLabelsOrNestedNames() {
        var nested = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(i64())).build();
        var schema = NamedStruct.newBuilder().addAllNames(List.of("same", "child", "same", "child"))
                .setStruct(Type.Struct.newBuilder().addTypes(nested).addTypes(nested)
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE).setTypeVariationReference(9)).build();
        var layout = SubstraitRelationLayout.describe(read(schema));
        assertThat(layout.outputSchema()).isEqualTo(schema);
        assertThat(layout.physicalSchema().getNamesList())
                .containsExactly("$pangrova$column_0", "child", "$pangrova$column_1", "child");
        assertThat(layout.physicalSchema().getStruct()).isEqualTo(schema.getStruct());
        assertThatThrownBy(() -> SubstraitRelationLayout.physicalAlias(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Rel read(NamedStruct schema) { return Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema)).build(); }
    private static NamedStruct flatSchema() {
        return NamedStruct.newBuilder().addNames("id").addNames("allowed")
                .setStruct(Type.Struct.newBuilder().addTypes(i64()).addTypes(bool())).build();
    }
    private static Type i64() { return SubstraitSchemaFields.primitive("i64", Type.Nullability.NULLABILITY_REQUIRED); }
    private static Type bool() { return SubstraitSchemaFields.primitive("bool", Type.Nullability.NULLABILITY_REQUIRED); }
    private static Expression integer(long value) { return Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(value)).build(); }
    private static Expression field(int ordinal) {
        return Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
                .setDirectReference(Expression.ReferenceSegment.newBuilder().setStructField(
                        Expression.ReferenceSegment.StructField.newBuilder().setField(ordinal)))).build();
    }
    private static RelCommon emit(int... indices) {
        var builder = RelCommon.Emit.newBuilder();
        for (int index : indices) builder.addOutputMapping(index);
        return RelCommon.newBuilder().setEmit(builder).build();
    }
}
