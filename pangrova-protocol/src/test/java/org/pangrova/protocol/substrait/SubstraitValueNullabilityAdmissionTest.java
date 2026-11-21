package org.pangrova.protocol.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Canonical declaration admission, not a Native result or model execution oracle. */
class SubstraitValueNullabilityAdmissionTest {
    @Test
    void readRejectsMissingLeafAndNestedValueDeclarationsWithoutRepairingInput() {
        Type missing = Type.newBuilder().setI64(Type.I64.newBuilder()).build();
        for (Type invalid : List.of(missing, row(missing, Type.Nullability.NULLABILITY_REQUIRED),
                list(missing), map(missing), row(integer(), Type.Nullability.NULLABILITY_UNSPECIFIED))) {
            NamedStruct input = schema(invalid);
            byte[] original = input.toByteArray();
            assertThatThrownBy(() -> SubstraitRelationLayout.describe(Rel.newBuilder()
                    .setRead(ReadRel.newBuilder().setBaseSchema(input)).build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
            assertThat(input.toByteArray()).containsExactly(original);
        }
    }

    @Test
    void everyDeclaredCallOutputRejectsMissingNullabilityBeforeTypePropagation() {
        Type missing = Type.newBuilder().setI64(Type.I64.newBuilder()).build();
        for (Expression invalid : List.of(
                Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder().setOutputType(missing)).build(),
                Expression.newBuilder().setWindowFunction(Expression.WindowFunction.newBuilder().setOutputType(missing)).build(),
                Expression.newBuilder().setCast(Expression.Cast.newBuilder().setType(missing)).build())) {
            assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(invalid, schema(integer())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
        }
    }

    @Test
    void caseAndSetCannotEraseMissingComplexParentOrUnknownEnum() {
        Type valid = row(integer(), Type.Nullability.NULLABILITY_NULLABLE);
        Type missing = valid.toBuilder().setStruct(valid.getStruct().toBuilder()
                .setNullability(Type.Nullability.NULLABILITY_UNSPECIFIED)).build();
        assertThatThrownBy(() -> SubstraitSetShape.merge(List.of(missing, valid)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
        var conditional = Expression.IfThen.newBuilder().addIfs(Expression.IfThen.IfClause.newBuilder()
                .setIf(Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setBoolean(true)))
                .setThen(Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder().setOutputType(missing))))
                .setElse(Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder().setOutputType(valid)));
        assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(
                Expression.newBuilder().setIfThen(conditional).build(), schema(integer())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
        Type unknown = integer().toBuilder().setI64(integer().getI64().toBuilder().setNullabilityValue(99)).build();
        assertThatThrownBy(() -> SubstraitSetShape.merge(List.of(integer(), unknown)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
    }

    @Test
    void namedStructContainerMetadataRemainsDistinctFromNullableRowValues() {
        var input = schema(row(integer(), Type.Nullability.NULLABILITY_NULLABLE));
        assertThat(input.getStruct().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_UNSPECIFIED);
        var output = SubstraitRelationLayout.describe(Rel.newBuilder()
                .setRead(ReadRel.newBuilder().setBaseSchema(input)).build()).outputSchema();
        assertThat(output).isEqualTo(input);
        assertThat(output.getStruct().getTypes(0).getStruct().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(output.getStruct().getTypes(0).getStruct().getTypes(0).getI64().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
        var selection = Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
                .setDirectReference(Expression.ReferenceSegment.newBuilder().setStructField(
                        Expression.ReferenceSegment.StructField.newBuilder().setField(0)))).build();
        NamedStruct flat = schema(integer());
        assertThat(SubstraitRelationLayout.typeOf(selection, flat)).isEqualTo(integer());
        assertThat(flat.getStruct().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_UNSPECIFIED);
        assertThat(SubstraitRelationLayout.typeOf(selection, input)).isEqualTo(input.getStruct().getTypes(0));
    }

    @Test
    void recursiveAdmissionChecksItsBoundBeforeDescendingIntoNames() {
        Type value = integer();
        for (int level = 0; level < 64; level++) value = list(value);
        assertThat(SubstraitExpressionTypes.declared(value)).isEqualTo(value);
        Type excessive = list(value);
        assertThatThrownBy(() -> SubstraitSchemaFields.fields(schema(excessive)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("validation bound");
    }

    private static Type integer() {
        return Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static Type row(Type child, Type.Nullability nullable) {
        return Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(nullable).addTypes(child)).build();
    }

    private static Type list(Type child) {
        return Type.newBuilder().setList(Type.List.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .setType(child)).build();
    }

    private static Type map(Type child) {
        return Type.newBuilder().setMap(Type.Map.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .setKey(integer()).setValue(child)).build();
    }

    private static NamedStruct schema(Type child) {
        var result = NamedStruct.newBuilder().addNames("value").setStruct(Type.Struct.newBuilder().addTypes(child));
        if (child.hasStruct()) result.addNames("nested_value");
        return result.build();
    }
}
