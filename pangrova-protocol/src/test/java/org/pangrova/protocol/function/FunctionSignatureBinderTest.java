package org.pangrova.protocol.function;

import com.google.protobuf.UnknownFieldSet;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionAnyType;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionOpaqueRegisteredType;
import org.pangrova.protocol.v2.FunctionSignatureContract;
import org.pangrova.protocol.v2.FunctionTypeFormula;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.FunctionVariableKind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.appendPath;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.decimal;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.primitive;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.projection;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.projector;

/** Interpreter boundaries only; constructed patterns do not advertise actual Worker capabilities. */
class FunctionSignatureBinderTest {
    @Test
    void repeatedTypeVariablesRequireExactProjectedTypeWithoutNumericWidening() {
        var signature = FunctionSignatureContract.newBuilder().addArguments(variableType("T"))
                .addArguments(variableType("T")).addConstantArguments(false).addConstantArguments(false)
                .setOutput(variableType("T"));
        signature.addVariablesBuilder().setName("T").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_TYPE);
        var binder = binder();
        assertThat(binder.bind(signature.build(), schema(primitive("i64"), primitive("i64"))).orElseThrow()
                .output()).isEqualTo(primitive("i64"));
        assertThat(binder.bind(signature.build(), schema(primitive("i64"), primitive("string")))).isEmpty();
    }

    @Test
    void integerInputConstantIsCheckedButDerivedFormulaNeverOverridesBoundInput() {
        var signature = decimalSignature();
        signature.getVariablesBuilder(0).setInputIntegerConstant(12);
        assertThat(binder().bind(signature.build(), schema(decimalType(13, 3)))).isEmpty();
        signature.getVariablesBuilder(0).clearInputIntegerConstant().setFormula(binary(
                FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_ADD, variable("p"), integer(1)));
        assertThat(binder().bind(signature.build(), schema(decimalType(38, 18))).orElseThrow()
                .output()).isEqualTo(decimalType(38, 18));
    }

    @Test
    void selectedResultFormulaFailurePropagatesInsteadOfInventingAResult() {
        var signature = decimalSignature();
        signature.addVariablesBuilder().setName("r").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER)
                .setFormula(binary(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_ADD, variable("p"), integer(1)));
        signature.getOutputBuilder().getSlotsBuilder(0).setInteger(variable("r"));
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(decimalType(38, 18))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not representable");
    }

    @Test
    void anyMarkerDoesNotAssignATypeVariableAndUnknownMarkerFieldsReject() {
        var signature = FunctionSignatureContract.newBuilder().addArguments(FunctionTypeTemplate.newBuilder()
                .setAnyType(FunctionAnyType.getDefaultInstance())).addConstantArguments(false).setOutput(variableType("T"));
        signature.addVariablesBuilder().setName("T").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_TYPE);
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(primitive("i64"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unbound registered type variable");
        signature.setOutput(FunctionTypeTemplate.newBuilder().setSkeleton(primitive("i64")));
        assertThat(binder().bind(signature.build(), schema(primitive("string")))).isPresent();
        var unknown = UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(0).build()).build();
        signature.getArgumentsBuilder(0).getAnyTypeBuilder().setUnknownFields(unknown);
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(primitive("i64"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown fields");
    }

    @Test
    void opaqueCustomAstRoundTripsNamesButNeverBecomesAnExecutableCanonicalType() throws Exception {
        var opaque = FunctionOpaqueRegisteredType.newBuilder().setName("registered_extension")
                .addParameters(FunctionOpaqueRegisteredType.newBuilder().setName("row").setVariableArity(true)
                        .addParameters(FunctionOpaqueRegisteredType.newBuilder().setName("T").setRowFieldName("")));
        var template = FunctionTypeTemplate.newBuilder().setOpaqueRegisteredType(opaque).build();
        var decoded = FunctionTypeTemplate.parseFrom(template.toByteArray());
        assertThat(decoded).isEqualTo(template);
        assertThat(decoded.getOpaqueRegisteredType().getParameters(0).getParameters(0).hasRowFieldName()).isTrue();
        var signature = FunctionSignatureContract.newBuilder().addArguments(template)
                .addConstantArguments(false).setOutput(FunctionTypeTemplate.newBuilder().setSkeleton(primitive("i64"))).build();
        assertThat(binder().bind(signature, schema(primitive("i64")))).isEmpty();
    }

    @Test
    void opaqueAstRecursionAndUnknownFieldsFailBeforeBecomingAQuietNonMatch() {
        var signature = FunctionSignatureContract.newBuilder().addConstantArguments(false)
                .setOutput(FunctionTypeTemplate.newBuilder().setSkeleton(primitive("i64")));
        var opaque = FunctionOpaqueRegisteredType.newBuilder().setName("custom");
        for (int index = 0; index < 66; index++) {
            opaque = FunctionOpaqueRegisteredType.newBuilder().setName("custom").addParameters(opaque);
        }
        signature.addArguments(FunctionTypeTemplate.newBuilder().setOpaqueRegisteredType(opaque));
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(primitive("i64"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exceeds its bounds");
    }

    @Test
    void explicitlyEmptyNamedRowConstraintIsNotEquivalentToAnUnconstrainedField() {
        var signature = rowSignature(false);
        signature.getArgumentsBuilder(0).addFieldNameConstraintsBuilder()
                .addAllPath(signature.getArguments(0).getSlots(0).getPathList()).setName("");
        var row = Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypes(primitive("i64"))).build();
        var empty = NamedStruct.newBuilder().addNames("tuple").addNames("").setStruct(Type.Struct.newBuilder().addTypes(row)).build();
        assertThat(binderWithRows().bind(signature.build(), empty)).isPresent();
        assertThat(binderWithRows().bind(signature.build(), empty.toBuilder().setNames(1, "named").build())).isEmpty();
    }

    @Test
    void homogeneousRowUnifiesEveryActualChildAndPreservesEmptyRowBinding() {
        var signature = rowSignature(true).build();
        var row = Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypes(primitive("i64")).addTypes(primitive("i64"))).build();
        var input = NamedStruct.newBuilder().addNames("tuple").addNames("first").addNames("second")
                .setStruct(Type.Struct.newBuilder().addTypes(row)).build();
        assertThat(binderWithRows().bind(signature, input)).isPresent();
        var mixed = input.toBuilder().setStruct(Type.Struct.newBuilder().addTypes(row.toBuilder()
                .setStruct(row.getStruct().toBuilder().setTypes(1, primitive("string"))))).build();
        assertThat(binderWithRows().bind(signature, mixed)).isEmpty();
        assertThat(binderWithRows().bind(signature, schema(primitive("struct")))).isPresent();
    }

    @Test
    void malformedSlotPathsAndUnspecifiedRootsDoNotDefaultToSupport() {
        var signature = decimalSignature();
        signature.getArgumentsBuilder(0).getSlotsBuilder(0).getPathBuilder(1).setFieldNumber(99999);
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(decimalType(12, 3))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown field");
        signature.getArgumentsBuilder(0).clear();
        assertThatThrownBy(() -> binder().bind(signature.build(), schema(decimalType(12, 3))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no explicit root");
    }

    private static FunctionSignatureBinder binder() {
        var decimal = decimal();
        decimal.addPredicates(binary(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_GREATER_OR_EQUAL,
                variable("p"), integer(1)));
        decimal.addPredicates(binary(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_LESS_OR_EQUAL,
                variable("p"), integer(38)));
        decimal.addPredicates(binary(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_GREATER_OR_EQUAL,
                variable("s"), integer(0)));
        decimal.addPredicates(binary(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_LESS_OR_EQUAL,
                variable("s"), variable("p")));
        return new FunctionSignatureBinder(projector(decimal.build(), projection(primitive("i64"), "BIGINT").build(),
                projection(primitive("string"), "VARCHAR").build()));
    }

    private static FunctionSignatureBinder binderWithRows() {
        var row = projection(primitive("struct"), "ROW");
        row.addRecursiveChildrenBuilder().addFieldNumbers(Type.getDescriptor().findFieldByName("struct").getNumber())
                .addFieldNumbers(Type.Struct.getDescriptor().findFieldByName("types").getNumber());
        return new FunctionSignatureBinder(projector(row.build(), projection(primitive("i64"), "BIGINT").build(),
                projection(primitive("string"), "VARCHAR").build()));
    }

    private static FunctionSignatureContract.Builder decimalSignature() {
        var template = FunctionTypeTemplate.newBuilder().setSkeleton(primitive("decimal"));
        for (String field : new String[]{"precision", "scale"}) {
            var slot = template.addSlotsBuilder();
            appendPath(slot, template.getSkeleton(), field);
            slot.setInteger(variable(field.equals("precision") ? "p" : "s"));
        }
        var signature = FunctionSignatureContract.newBuilder().addArguments(template).setOutput(template).addConstantArguments(false);
        signature.addVariablesBuilder().setName("p").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER);
        signature.addVariablesBuilder().setName("s").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER);
        return signature;
    }

    private static FunctionSignatureContract.Builder rowSignature(boolean homogeneous) {
        var row = Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypes(primitive("i64"))).build();
        var template = FunctionTypeTemplate.newBuilder().setSkeleton(row).setHomogeneousRow(homogeneous);
        var slot = template.addSlotsBuilder().setType(variableType("T"));
        slot.addPathBuilder().setFieldNumber(Type.getDescriptor().findFieldByName("struct").getNumber());
        slot.addPathBuilder().setFieldNumber(Type.Struct.getDescriptor().findFieldByName("types").getNumber()).setRepeatedIndex(0);
        var signature = FunctionSignatureContract.newBuilder().addArguments(template).addConstantArguments(false)
                .setOutput(FunctionTypeTemplate.newBuilder().setSkeleton(primitive("i64")));
        signature.addVariablesBuilder().setName("T").setKind(FunctionVariableKind.FUNCTION_VARIABLE_KIND_TYPE);
        return signature;
    }

    private static NamedStruct schema(Type... types) {
        var schema = NamedStruct.newBuilder();
        for (int index = 0; index < types.length; index++) schema.addNames("argument_" + index);
        return schema.setStruct(Type.Struct.newBuilder().addAllTypes(java.util.List.of(types))).build();
    }

    private static Type decimalType(int precision, int scale) {
        return Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(precision).setScale(scale)
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static FunctionTypeTemplate variableType(String name) { return FunctionTypeTemplate.newBuilder().setTypeVariable(name).build(); }
    private static FunctionTypeFormula variable(String name) {
        return FunctionTypeFormula.newBuilder().setOperation(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE)
                .setVariableName(name).build();
    }
    private static FunctionTypeFormula integer(long value) {
        return FunctionTypeFormula.newBuilder().setOperation(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER).setIntegerValue(value).build();
    }
    private static FunctionTypeFormula binary(FunctionFormulaOperation operation, FunctionTypeFormula left, FunctionTypeFormula right) {
        return FunctionTypeFormula.newBuilder().setOperation(operation).addOperands(left).addOperands(right).build();
    }
}
