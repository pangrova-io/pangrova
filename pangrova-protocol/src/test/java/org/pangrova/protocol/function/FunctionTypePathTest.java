package org.pangrova.protocol.function;

import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionTypePathStep;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionTypePathTest {
    @Test
    void decimalParameterReplacementPreservesAllOtherDeclaredMetadata() {
        Type decimal = decimal();
        List<FunctionTypePathStep> path = List.of(step(Type.DECIMAL_FIELD_NUMBER),
                step(Type.Decimal.PRECISION_FIELD_NUMBER));
        var original = FunctionTypePath.read(decimal, path);
        assertThat(original.value()).isEqualTo(38);
        var replacement = (Type) FunctionTypePath.replace(decimal, path, 37);
        assertThat(replacement).isEqualTo(decimal.toBuilder()
                .setDecimal(decimal.getDecimal().toBuilder().setPrecision(37)).build());
        assertThat(decimal.getDecimal().getPrecision()).isEqualTo(38);
    }

    @Test
    void nestedCollectionReplacementUsesDescriptorNumbersAndExactRepeatedOrdinal() {
        Type row = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(decimal())
                .addTypes(Type.newBuilder().setList(Type.List.newBuilder().setType(decimal())))).build();
        var path = List.of(step(Type.STRUCT_FIELD_NUMBER), indexed(Type.Struct.TYPES_FIELD_NUMBER, 1),
                step(Type.LIST_FIELD_NUMBER), step(Type.List.TYPE_FIELD_NUMBER));
        assertThat(FunctionTypePath.read(row, path).value()).isEqualTo(decimal());
        var scalar = Type.newBuilder().setFp64(Type.FP64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        var changed = (Type) FunctionTypePath.replace(row, path, scalar);
        assertThat(changed.getStruct().getTypes(0)).isEqualTo(decimal());
        assertThat(changed.getStruct().getTypes(1).getList().getType()).isEqualTo(scalar);
        assertThat(row.getStruct().getTypes(1).getList().getType()).isEqualTo(decimal());
    }

    @Test
    void malformedPathsCannotTraverseMissingDefaultMessagesOrImplicitIndices() {
        List<List<FunctionTypePathStep>> paths = List.of(List.of(),
                List.of(step(999)), List.of(step(Type.LIST_FIELD_NUMBER)),
                List.of(indexed(Type.DECIMAL_FIELD_NUMBER, 0)),
                List.of(step(Type.DECIMAL_FIELD_NUMBER), step(Type.Decimal.PRECISION_FIELD_NUMBER), step(1)));
        for (var path : paths) {
            assertThatThrownBy(() -> FunctionTypePath.read(decimal(), path))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        Type row = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(decimal())).build();
        for (var child : List.of(step(Type.Struct.TYPES_FIELD_NUMBER),
                indexed(Type.Struct.TYPES_FIELD_NUMBER, -1), indexed(Type.Struct.TYPES_FIELD_NUMBER, 1))) {
            assertThatThrownBy(() -> FunctionTypePath.read(row, List.of(step(Type.STRUCT_FIELD_NUMBER), child)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static Type decimal() {
        return Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(38).setScale(18)
                .setTypeVariationReference(17).setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
    }

    private static FunctionTypePathStep step(int number) {
        return FunctionTypePathStep.newBuilder().setFieldNumber(number).build();
    }

    private static FunctionTypePathStep indexed(int number, int index) {
        return step(number).toBuilder().setRepeatedIndex(index).build();
    }
}
