package org.pangrova.types.mapping;

import org.pangrova.types.enums.VectorElementType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.RowType;
import org.pangrova.types.Type;
import org.pangrova.types.VectorType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for VECTOR type support in {@link PangrovaTypeMapping}:
 * toType(VECTOR) returns null, toPangrovaType(VectorType) returns VECTOR,
 * and fromSqlTypeName parses VECTOR(dim[, elementType]) into VectorType.
 */
class PangrovaTypeMappingVectorTest {

    @Test
    void toTypeVectorReturnsNull() {
        assertThat(PangrovaTypeMapping.toType(PangrovaType.VECTOR)).isNull();
    }

    @Test
    void toPangrovaTypeVectorMapping() {
        VectorType vectorType = new VectorType(128, VectorElementType.FLOAT);
        assertThat(PangrovaTypeMapping.toPangrovaType(vectorType)).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void toPangrovaTypeVectorDoubleMapping() {
        VectorType vectorType = new VectorType(384, VectorElementType.DOUBLE);
        assertThat(PangrovaTypeMapping.toPangrovaType(vectorType)).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void fromSqlTypeNameVectorDefaultFloat() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("VECTOR(128)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(128);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorExplicitFloat() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("VECTOR(768, FLOAT)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(768);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorDouble() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("VECTOR(384, DOUBLE)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.DOUBLE);
    }

    @Test
    void fromSqlTypeNameVectorCaseInsensitive() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("vector(256)");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).dimension()).isEqualTo(256);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "VECTOR(128)",
            "VECTOR( 128 )"
    })
    void fromSqlTypeNameVectorWhitespaceHandling(String sqlType) {
        Type result = PangrovaTypeMapping.fromSqlTypeName(sqlType);
        assertThat(result).isInstanceOf(VectorType.class);
    }

    @Test
    void fromSqlTypeNameVectorWithSpacesAroundComma() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("VECTOR(128 , FLOAT)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(128);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorDoubleWithSpaces() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("VECTOR( 384 , DOUBLE )");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.DOUBLE);
    }

    @Test
    void fromSqlTypeNameVectorNoDimensionThrows() {
        assertThatThrownBy(() -> PangrovaTypeMapping.fromSqlTypeName("VECTOR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension parameter");
    }

    @Test
    void fromSqlTypeNameVectorInvalidDimensionThrows() {
        assertThatThrownBy(() -> PangrovaTypeMapping.fromSqlTypeName("VECTOR(abc)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid VECTOR dimension");
    }

    @Test
    void fromSqlTypeNameVectorUnsupportedElementTypeThrows() {
        assertThatThrownBy(() -> PangrovaTypeMapping.fromSqlTypeName("VECTOR(128, INT)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported VECTOR element type");
    }

    @Test
    void fromSqlTypeNameVectorDimensionOutOfRangeThrows() {
        assertThatThrownBy(() -> PangrovaTypeMapping.fromSqlTypeName("VECTOR(0)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromSqlTypeNameNonVectorDelegates() {
        Type result = PangrovaTypeMapping.fromSqlTypeName("BIGINT");
        assertThat(result).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"VECTOR_EXTRA(128)", "VECTOR(128) trailing", "VECTOR(128))", "VECTOR(128)()"})
    void malformedVectorDeclarationIsRejected(String declaration) {
        assertThatThrownBy(() -> PangrovaTypeMapping.fromSqlTypeName(declaration))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @ResourceLock(org.junit.jupiter.api.parallel.Resources.LOCALE)
    void parsingDoesNotDependOnDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertThat(PangrovaTypeMapping.fromSqlTypeName("file")).isInstanceOf(RowType.class);
            assertThat(PangrovaType.fromSqlTypeName("bigint")).isEqualTo(PangrovaType.BIGINT);
        } finally {
            Locale.setDefault(previous);
        }
    }
}
