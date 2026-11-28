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
 * Tests for VECTOR type support in {@link PangrovaTypeConverter}:
 * toType(VECTOR) returns null, toPangrovaType(VectorType) returns VECTOR,
 * and fromSqlTypeName parses VECTOR(dim[, elementType]) into VectorType.
 */
class PangrovaTypeConverterVectorTest {

    @Test
    void toTypeVectorReturnsNull() {
        assertThat(PangrovaTypeConverter.toType(PangrovaType.VECTOR)).isNull();
    }

    @Test
    void toPangrovaTypeVectorMapping() {
        VectorType vectorType = new VectorType(128, VectorElementType.FLOAT);
        assertThat(PangrovaTypeConverter.toPangrovaType(vectorType)).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void toPangrovaTypeVectorDoubleMapping() {
        VectorType vectorType = new VectorType(384, VectorElementType.DOUBLE);
        assertThat(PangrovaTypeConverter.toPangrovaType(vectorType)).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void fromSqlTypeNameVectorDefaultFloat() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("VECTOR(128)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(128);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorExplicitFloat() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("VECTOR(768, FLOAT)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(768);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorDouble() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("VECTOR(384, DOUBLE)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.DOUBLE);
    }

    @Test
    void fromSqlTypeNameVectorCaseInsensitive() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("vector(256)");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).dimension()).isEqualTo(256);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "VECTOR(128)",
            "VECTOR( 128 )"
    })
    void fromSqlTypeNameVectorWhitespaceHandling(String sqlType) {
        Type result = PangrovaTypeConverter.fromSqlTypeName(sqlType);
        assertThat(result).isInstanceOf(VectorType.class);
    }

    @Test
    void fromSqlTypeNameVectorWithSpacesAroundComma() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("VECTOR(128 , FLOAT)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(128);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.FLOAT);
    }

    @Test
    void fromSqlTypeNameVectorDoubleWithSpaces() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("VECTOR( 384 , DOUBLE )");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorElementType.DOUBLE);
    }

    @Test
    void fromSqlTypeNameVectorNoDimensionThrows() {
        assertThatThrownBy(() -> PangrovaTypeConverter.fromSqlTypeName("VECTOR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension parameter");
    }

    @Test
    void fromSqlTypeNameVectorInvalidDimensionThrows() {
        assertThatThrownBy(() -> PangrovaTypeConverter.fromSqlTypeName("VECTOR(abc)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid VECTOR dimension");
    }

    @Test
    void fromSqlTypeNameVectorUnsupportedElementTypeThrows() {
        assertThatThrownBy(() -> PangrovaTypeConverter.fromSqlTypeName("VECTOR(128, INT)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported VECTOR element type");
    }

    @Test
    void fromSqlTypeNameVectorDimensionOutOfRangeThrows() {
        assertThatThrownBy(() -> PangrovaTypeConverter.fromSqlTypeName("VECTOR(0)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromSqlTypeNameNonVectorDelegates() {
        Type result = PangrovaTypeConverter.fromSqlTypeName("BIGINT");
        assertThat(result).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"VECTOR_EXTRA(128)", "VECTOR(128) trailing", "VECTOR(128))", "VECTOR(128)()"})
    void malformedVectorDeclarationIsRejected(String declaration) {
        assertThatThrownBy(() -> PangrovaTypeConverter.fromSqlTypeName(declaration))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @ResourceLock(org.junit.jupiter.api.parallel.Resources.LOCALE)
    void parsingDoesNotDependOnDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertThat(PangrovaTypeConverter.fromSqlTypeName("file")).isInstanceOf(RowType.class);
            assertThat(PangrovaType.fromSqlTypeName("bigint")).isEqualTo(PangrovaType.BIGINT);
        } finally {
            Locale.setDefault(previous);
        }
    }
}
