package org.pangrova.protocol.type;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.data.type.TypeSignature;
import org.pangrova.connector.spi.data.type.DeclaredType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeclaredLogicalTypesTest {
    @Test
    void uuidDeclarationsPreserveIdentityAndNullabilityWithinContainers() {
        var uuid = scalar("UUID", false);
        var value = DeclaredLogicalTypes.logicalType(uuid);
        assertThat(value.getCanonicalSql()).isEqualTo("UUID");
        assertThat(value.getSubstraitType().getUuid().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        var array = new DeclaredType(new TypeSignature("ARRAY", List.of(
                TypeSignature.TypeParameter.ofType(uuid.signature()))), true, List.of(uuid));
        var nested = DeclaredLogicalTypes.logicalType(array);
        assertThat(nested.getCanonicalSql()).isEqualTo("ARRAY(UUID)");
        assertThat(nested.getSubstraitType().getList().getType().hasUuid()).isTrue();
        assertThat(nested.getSubstraitType().getList().getType().getUuid().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThatThrownBy(() -> DeclaredLogicalTypes.logicalType(new DeclaredType(
                new TypeSignature("UUID", List.of(TypeSignature.TypeParameter.ofLong(16))), true, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEveryCompositeSignatureChildMismatch() {
        var integer = scalar("INTEGER", false);
        var string = scalar("VARCHAR", true);
        var array = new TypeSignature("ARRAY", List.of(TypeSignature.TypeParameter.ofType(integer.signature())));
        assertThatThrownBy(() -> DeclaredLogicalTypes.logicalType(new DeclaredType(array, true, List.of(string))))
                .isInstanceOf(IllegalArgumentException.class);
        var map = new TypeSignature("MAP", List.of(TypeSignature.TypeParameter.ofType(integer.signature()),
                TypeSignature.TypeParameter.ofType(string.signature())));
        assertThatThrownBy(() -> DeclaredLogicalTypes.logicalType(new DeclaredType(map, true, List.of(string, integer))))
                .isInstanceOf(IllegalArgumentException.class);
        var row = new TypeSignature("ROW", List.of(TypeSignature.TypeParameter.ofNamedType("id", integer.signature())));
        assertThatThrownBy(() -> DeclaredLogicalTypes.logicalType(new DeclaredType(row, true, List.of(string))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nestedVectorDimensionsPrecisionFieldNamesAndNullabilityRemainExplicit() {
        var element = scalar("DOUBLE", true);
        var vector = new DeclaredType(new TypeSignature("VECTOR", List.of(TypeSignature.TypeParameter.ofLong(3),
                TypeSignature.TypeParameter.ofType(element.signature()))), false, List.of(element));
        var row = new DeclaredType(new TypeSignature("ROW", List.of(TypeSignature.TypeParameter.ofNamedType(
                "quoted\"field", vector.signature()))), true, List.of(vector));
        var logical = DeclaredLogicalTypes.logicalType(row);
        assertThat(logical.getCanonicalSql()).isEqualTo("ROW(\"quoted\"\"field\" VECTOR(3,DOUBLE))");
        assertThat(logical.getConstraintsList()).anySatisfy(constraint -> {
            assertThat(constraint.getName()).isEqualTo("field_name@0");
            assertThat(constraint.getStringValue()).isEqualTo("quoted\"field");
        }).anySatisfy(constraint -> {
            assertThat(constraint.getName()).isEqualTo("dimension@0");
            assertThat(constraint.getIntegerValue()).isEqualTo(3);
        }).anySatisfy(constraint -> {
            assertThat(constraint.getName()).isEqualTo("element_type@0");
            assertThat(constraint.getStringValue()).isEqualTo(VectorLogicalType.FLOAT64);
        });
        var physical = logical.getSubstraitType().getStruct().getTypes(0).getList();
        assertThat(physical.getNullability()).isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(physical.getType().getFp64().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
    }

    @Test
    void rejectsMalformedScalarParametersWithoutAHashBasedEscape() {
        for (var signature : List.of(
                new TypeSignature("INTEGER", List.of(TypeSignature.TypeParameter.ofLong(7))),
                new TypeSignature("DECIMAL", List.of(TypeSignature.TypeParameter.ofLong(38),
                        TypeSignature.TypeParameter.ofLong(39))),
                new TypeSignature("TIMESTAMP", List.of(TypeSignature.TypeParameter.ofLong(10))),
                new TypeSignature("CHAR", List.of(TypeSignature.TypeParameter.ofLong(0))),
                new TypeSignature("VARCHAR", List.of(new TypeSignature.TypeParameter(27L,
                        new TypeSignature("INTEGER"), null))))) {
            assertThatThrownBy(() -> DeclaredLogicalTypes.logicalType(new DeclaredType(signature, true, List.of())))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void nestedFloatVectorUsesTheSameCanonicalSpellingAsRootVector() {
        var element = scalar("FLOAT", false);
        var vector = new DeclaredType(new TypeSignature("VECTOR", List.of(TypeSignature.TypeParameter.ofLong(3),
                TypeSignature.TypeParameter.ofType(element.signature()))), true, List.of(element));
        var array = new DeclaredType(new TypeSignature("ARRAY",
                List.of(TypeSignature.TypeParameter.ofType(vector.signature()))), true, List.of(vector));
        assertThat(DeclaredLogicalTypes.logicalType(vector).getCanonicalSql()).isEqualTo("VECTOR(3)");
        assertThat(DeclaredLogicalTypes.logicalType(array).getCanonicalSql()).isEqualTo("ARRAY(VECTOR(3))");
    }

    private static DeclaredType scalar(String base, boolean nullable) {
        return new DeclaredType(new TypeSignature(base), nullable, List.of());
    }
}
