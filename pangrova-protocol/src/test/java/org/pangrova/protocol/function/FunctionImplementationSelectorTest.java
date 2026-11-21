package org.pangrova.protocol.function;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.function.selection.FunctionImplementationSelector;
import org.pangrova.protocol.v2.FunctionImplementationContract;
import org.pangrova.protocol.v2.FunctionImplementationFamily;
import org.pangrova.protocol.v2.FunctionPhysicalTypePattern;
import org.pangrova.protocol.v2.FunctionSignatureContract;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.FunctionTypeProjectionChildPath;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.primitive;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.projection;
import static org.pangrova.protocol.function.FunctionPhysicalTypeProjectorTest.projector;

/** Interpreter boundary tests; these declarations do not advertise a Native factory or SQL capability. */
class FunctionImplementationSelectorTest {
    @Test
    void vectorPrecedesLowerPrioritySimpleWithoutSearchingForARequestedOutput() {
        var simple = simple(0, 0, "BIGINT", "BIGINT");
        var vector = vector(1, "i32");
        var selected = selector().select(List.of(simple, vector), arguments(), Set.of()).orElseThrow();
        assertThat(selected.implementation()).isEqualTo(vector);
        assertThat(selected.binding().output().getKindCase()).isEqualTo(Type.KindCase.I32);
    }

    @Test
    void simplePhysicalInputAndOutputAreCheckedBeforeMinimumPriority() {
        var wrongInput = simple(0, 0, "INTEGER", "BIGINT");
        var wrongOutput = simple(1, 0, "BIGINT", "INTEGER");
        var exact = simple(2, 100, "BIGINT", "BIGINT");
        assertThat(selector().select(List.of(wrongInput, wrongOutput, exact), arguments(), Set.of())
                .orElseThrow().implementation()).isEqualTo(exact);
    }

    @Test
    void equalPriorityUsesActualTraversalAndUnsignedPriorityIsNotNegative() {
        var first = simple(0, 3, "UNKNOWN", "UNKNOWN");
        var second = simple(1, 3, "BIGINT", "BIGINT");
        var unsigned = simple(2, -1, "BIGINT", "BIGINT");
        assertThat(selector().select(List.of(first, second, unsigned), arguments(), Set.of())
                .orElseThrow().implementation()).isEqualTo(first);
    }

    @Test
    void selectedConstantFailureDoesNotChooseAnotherMatchingImplementation() {
        var constant = vector(0, "i64").toBuilder();
        constant.getSignatureBuilder().setConstantArguments(0, true);
        var later = vector(1, "i64");
        assertThatThrownBy(() -> selector().select(List.of(constant.build(), later), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires a constant");
        assertThat(selector().select(List.of(constant.build(), later), arguments(), Set.of(0))
                .orElseThrow().implementation()).isEqualTo(constant.build());
    }

    @Test
    void selectedUnrepresentableResultPropagatesInsteadOfFallingThrough() {
        var invalid = vector(0, "string");
        var later = vector(1, "i64");
        assertThatThrownBy(() -> selector().select(List.of(invalid, later), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no actual Native");
    }

    @Test
    void opaqueSpecialNeverFallsThroughToAnExecutableNameSibling() {
        var special = FunctionImplementationContract.newBuilder()
                .setFamily(FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM).build();
        assertThatThrownBy(() -> selector().select(List.of(special, vector(1, "i64")), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("owned type program");
    }

    @Test
    void malformedOrderPhysicalConstraintsAndConstantOrdinalsAreRejected() {
        assertThatThrownBy(() -> selector().select(List.of(vector(1, "i64")), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("traversal");
        var malformed = simple(0, 0, "UNKNOWN", "BIGINT").toBuilder();
        malformed.getArgumentPhysicalBuilder(0).addChildren(pattern("BIGINT"));
        assertThatThrownBy(() -> selector().select(List.of(malformed.build()), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("physical constraint");
        assertThatThrownBy(() -> selector().select(List.of(vector(0, "i64")), arguments(), Set.of(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outside");
    }

    @Test
    void exactStructuralCoalesceUsesSourceOwnedNestedNamesNotRuntimeEquivalentRows() {
        var rowProjection = projection(primitive("struct"), "ROW");
        rowProjection.addRecursiveChildren(FunctionTypeProjectionChildPath.newBuilder()
                .addFieldNumbers(Type.getDescriptor().findFieldByName("struct").getNumber())
                .addFieldNumbers(Type.Struct.getDescriptor().findFieldByName("types").getNumber()));
        var exact = new FunctionImplementationSelector(projector(rowProjection.build(),
                projection(primitive("i64"), "BIGINT").build()));
        var row = Type.newBuilder().setStruct(Type.Struct.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).addTypes(primitive("i64"))).build();
        var arguments = NamedStruct.newBuilder().addNames("left").addNames("a")
                .addNames("right").addNames("a").setStruct(Type.Struct.newBuilder().addTypes(row).addTypes(row));
        var special = special("EXACT_SEQUENCE");
        var unspecified = arguments.clone();
        unspecified.getStructBuilder().getTypesBuilder(0).getStructBuilder().clearNullability();
        assertThatThrownBy(() -> exact.select(List.of(special), unspecified.build(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit nullability");
        assertThat(exact.select(List.of(special), arguments.build(), Set.of())
                .orElseThrow().binding().output()).isEqualTo(row);
        arguments.setNames(3, "b");
        assertThatThrownBy(() -> exact.select(List.of(special), arguments.build(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("input contract mismatch");
    }

    @Test
    void structuralCastRequiresItsExactTargetAndCannotSelectByDesiredOutput() {
        var source = selector();
        assertThat(source.select(List.of(special("DECLARED_TARGET_CHECKED")), arguments(), Set.of(), primitive("i32"))
                .orElseThrow().binding().output()).isEqualTo(primitive("i32"));
        assertThatThrownBy(() -> source.select(List.of(special("DECLARED_TARGET_CHECKED")), arguments(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit declared target");
    }

    @Test
    void structuralArityErrorDoesNotRestartAtMatchingVectorSibling() {
        var one = special("EXACT_SEQUENCE");
        var empty = NamedStruct.newBuilder().setStruct(Type.Struct.newBuilder()).build();
        assertThatThrownBy(() -> selector().select(List.of(one, vector(1, "i64")), empty, Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("arity");
    }

    private static FunctionImplementationContract special(String programme) {
        return FunctionImplementationContract.newBuilder()
                .setFamily(FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM)
                .setSpecialTypeProgram(programme).build();
    }

    private static FunctionImplementationSelector selector() {
        return new FunctionImplementationSelector(projector(projection(primitive("i64"), "BIGINT").build(),
                projection(primitive("i32"), "INTEGER").build()));
    }

    private static NamedStruct arguments() {
        return NamedStruct.newBuilder().addNames("input").setStruct(Type.Struct.newBuilder()
                .addTypes(primitive("i64"))).build();
    }

    private static FunctionImplementationContract vector(int ordinal, String output) {
        return FunctionImplementationContract.newBuilder().setTraversalOrdinal(ordinal)
                .setFamily(FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_VECTOR)
                .setSignature(signature(output)).build();
    }

    private static FunctionImplementationContract simple(int ordinal, int priority, String input, String output) {
        return vector(ordinal, "i64").toBuilder()
                .setFamily(FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SCALAR)
                .setPriority(priority).addArgumentPhysical(pattern(input)).setOutputPhysical(pattern(output)).build();
    }

    private static FunctionPhysicalTypePattern pattern(String kind) {
        return FunctionPhysicalTypePattern.newBuilder().setRuntimeTag(kind).build();
    }

    private static FunctionSignatureContract signature(String output) {
        return FunctionSignatureContract.newBuilder().addArguments(FunctionTypeTemplate.newBuilder()
                .setSkeleton(primitive("i64"))).addConstantArguments(false)
                .setOutput(FunctionTypeTemplate.newBuilder().setSkeleton(primitive(output))).build();
    }
}
