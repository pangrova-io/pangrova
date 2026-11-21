package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionPhysicalTypeProjection;
import org.pangrova.protocol.v2.FunctionTypeFormula;
import org.pangrova.protocol.v2.FunctionTypeProjectionChildPath;
import org.pangrova.protocol.v2.FunctionTypeTraitRule;
import org.pangrova.protocol.v2.FunctionVariableKind;
import org.pangrova.protocol.v2.FunctionCatalogManifest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Typed interpreter tests only; constructed metadata does not claim Native availability. */
class FunctionPhysicalTypeProjectorTest {
    @Test
    void exactDecimalParametersSurviveBindingWhileSqlNullabilityIsNotRuntimeIdentity() {
        var projection = decimal();
        var projector = projector(projection.build());
        for (int precision = 1; precision <= 38; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                Type input = Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(precision)
                        .setScale(scale).setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
                var actual = projector.project(input);
                assertThat(actual.bindingType().getDecimal().getPrecision()).isEqualTo(precision);
                assertThat(actual.bindingType().getDecimal().getScale()).isEqualTo(scale);
                assertThat(actual.bindingType().getDecimal().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
                assertThat(input.getDecimal().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
            }
        }
    }

    @Test
    void heterogeneousRowChildrenProjectIndependentlyInActualDeclarationOrder() {
        var row = projection(primitive("struct"), "ROW");
        row.addRecursiveChildren(childPath("struct", "types"));
        row.setOrderable(FunctionTypeTraitRule.FUNCTION_TYPE_TRAIT_RULE_ALL_CHILDREN);
        var varchar = projection(primitive("varchar"), "VARCHAR");
        varchar.getBindingTypeBuilder().setSkeleton(primitive("string"));
        parameter(varchar, "length", "length");
        var projector = projector(row.build(), projection(primitive("i64"), "BIGINT").build(),
                varchar.build(), decimal().build());
        Type input = Type.newBuilder().setStruct(Type.Struct.newBuilder()
                .addTypes(primitive("i64"))
                .addTypes(Type.newBuilder().setVarchar(Type.VarChar.newBuilder().setLength(27)))
                .addTypes(Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(38).setScale(18))))
                .build();
        var actual = projector.project(input);
        assertThat(actual.bindingType().getStruct().getTypesList()).hasSize(3);
        assertThat(actual.bindingType().getStruct().getTypes(1).getKindCase()).isEqualTo(Type.KindCase.STRING);
        assertThat(actual.bindingType().getStruct().getTypes(2).getDecimal().getPrecision()).isEqualTo(38);
        assertThat(actual.physicalType().getChildrenList()).extracting(child -> child.getRuntimeTag())
                .containsExactly("BIGINT", "VARCHAR", "HUGEINT");
        assertThat(actual.orderable()).isTrue();
    }

    @Test
    void absentChildrenAndOverlappingVariantsNeverDefaultToSupport() {
        var array = projection(primitive("list"), "ARRAY");
        array.addRecursiveChildren(childPath("list", "type"));
        assertThatThrownBy(() -> projector(array.build()).project(primitive("list")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("child is absent");
        var bigint = projection(primitive("i64"), "BIGINT").build();
        assertThatThrownBy(() -> projector(bigint, bigint).project(primitive("i64")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("overlap");
        assertThatThrownBy(() -> projector(bigint).project(primitive("string")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no actual Native");
    }

    @Test
    void malformedBindingsTraitsAndRecursivePathsFailBeforeProjection() {
        var decimal = decimal();
        decimal.getVariablesBuilder(0).setName("unbound");
        assertThatThrownBy(() -> projector(decimal.build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unbound");
        var row = projection(primitive("struct"), "ROW");
        row.addRecursiveChildren(childPath("struct", "types"));
        row.addRecursiveChildren(childPath("struct", "types"));
        assertThatThrownBy(() -> projector(row.build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Repeated recursive");
        var unknown = projection(primitive("i64"), "BIGINT").setKnownTypeValue(999);
        assertThatThrownBy(() -> projector(unknown.build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown actual Native type trait");
    }

    static FunctionPhysicalTypeProjector projector(FunctionPhysicalTypeProjection... declarations) {
        return new FunctionPhysicalTypeProjector(FunctionCatalogManifest.newBuilder()
                .addAllPhysicalTypeProjections(List.of(declarations)).build());
    }

    static FunctionPhysicalTypeProjection.Builder decimal() {
        var result = projection(primitive("decimal"), "HUGEINT");
        parameter(result, "p", "precision");
        parameter(result, "s", "scale");
        bindingParameter(result, "p", "precision");
        bindingParameter(result, "s", "scale");
        return result;
    }

    static FunctionPhysicalTypeProjection.Builder projection(Type type, String nativeName) {
        var result = FunctionPhysicalTypeProjection.newBuilder().setNativeTypeName(nativeName)
                .setKnownType(FunctionTypeTraitRule.FUNCTION_TYPE_TRAIT_RULE_ALWAYS)
                .setOrderable(FunctionTypeTraitRule.FUNCTION_TYPE_TRAIT_RULE_ALWAYS)
                .setComparable(FunctionTypeTraitRule.FUNCTION_TYPE_TRAIT_RULE_ALWAYS);
        result.getLogicalTypeBuilder().setSkeleton(type);
        result.getBindingTypeBuilder().setSkeleton(type);
        result.getPhysicalTypeBuilder().setRuntimeTag(nativeName);
        return result;
    }

    private static void parameter(FunctionPhysicalTypeProjection.Builder projection, String variable, String field) {
        projection.addVariablesBuilder().setName(variable).setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER);
        var slot = projection.getLogicalTypeBuilder().addSlotsBuilder();
        appendPath(slot, projection.getLogicalType().getSkeleton(), field);
        slot.setInteger(variable(variable));
    }

    private static void bindingParameter(FunctionPhysicalTypeProjection.Builder projection, String variable, String field) {
        var slot = projection.getBindingTypeBuilder().addSlotsBuilder();
        appendPath(slot, projection.getBindingType().getSkeleton(), field);
        slot.setInteger(variable(variable));
    }

    static void appendPath(org.pangrova.protocol.v2.FunctionTypeSlot.Builder slot, Type root, String fieldName) {
        var kind = FunctionTypePath.kind(root);
        slot.addPathBuilder().setFieldNumber(kind.getNumber());
        slot.addPathBuilder().setFieldNumber(kind.getMessageType().findFieldByName(fieldName).getNumber());
    }

    private static FunctionTypeFormula variable(String name) {
        return FunctionTypeFormula.newBuilder().setOperation(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE)
                .setVariableName(name).build();
    }

    private static FunctionTypeProjectionChildPath childPath(String kind, String child) {
        var field = Type.getDescriptor().findFieldByName(kind);
        return FunctionTypeProjectionChildPath.newBuilder().addFieldNumbers(field.getNumber())
                .addFieldNumbers(field.getMessageType().findFieldByName(child).getNumber()).build();
    }

    static Type primitive(String kind) {
        var builder = Type.newBuilder();
        FieldDescriptor field = Type.getDescriptor().findFieldByName(kind);
        var child = builder.newBuilderForField(field);
        child.setField(child.getDescriptorForType().findFieldByName("nullability"),
                Type.Nullability.NULLABILITY_REQUIRED.getValueDescriptor());
        return builder.setField(field, child.build()).build();
    }
}
