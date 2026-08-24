package org.pangrova.connector.paimon.type;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for PaimonTypeMapper — verifies bidirectional type mapping
 * between Paimon DataTypes and PangrovaType, plus RowType-to-column conversion.
 */
class PaimonTypeMapperTest {

    @ParameterizedTest(name = "Paimon {0} -> PangrovaType.{1}")
    @MethodSource("primitiveTypeMappings")
    void toPangrovaType_primitives(DataType paimonType, PangrovaType expected) {
        assertThat(PaimonTypeMapper.toPangrovaType(paimonType)).isEqualTo(expected);
    }

    @Test
    void toPangrovaType_array() {
        DataType array = new ArrayType(new IntType());
        assertThat(PaimonTypeMapper.toPangrovaType(array)).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_map() {
        DataType map = new MapType(new VarCharType(), new BigIntType());
        assertThat(PaimonTypeMapper.toPangrovaType(map)).isEqualTo(PangrovaType.MAP);
    }

    @Test
    void toPangrovaType_row() {
        DataType row = RowType.of(new IntType(), new VarCharType());
        assertThat(PaimonTypeMapper.toPangrovaType(row)).isEqualTo(PangrovaType.STRUCT);
    }

    @Test
    void toPangrovaType_multiset() {
        DataType multiset = new MultisetType(new VarCharType());
        assertThat(PaimonTypeMapper.toPangrovaType(multiset)).isEqualTo(PangrovaType.MAP);
    }

    @ParameterizedTest(name = "PangrovaType.{0} -> Paimon type class {1}")
    @MethodSource("reverseTypeMappings")
    void toPaimonType_primitives(PangrovaType pangrovaType, Class<? extends DataType> expectedClass) {
        DataType result = PaimonTypeMapper.toPaimonType(pangrovaType);
        assertThat(result).isInstanceOf(expectedClass);
    }

    @Test
    void toPaimonType_unsupported_throws() {
        assertThatThrownBy(() -> PaimonTypeMapper.toPaimonType(PangrovaType.STRUCT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported PangrovaType type: STRUCT");
    }

    @Test
    void toPaimonType_uuid_mapsToVarChar() {
        DataType result = PaimonTypeMapper.toPaimonType(PangrovaType.UUID);
        assertThat(result).isInstanceOf(VarCharType.class);
    }

    @Test
    void toPaimonType_fixed_mapsToBinary() {
        DataType result = PaimonTypeMapper.toPaimonType(PangrovaType.FIXED);
        assertThat(result).isInstanceOf(BinaryType.class);
    }

    @Test
    void toPangrovaColumns_preservesFieldIds() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType()),
                new DataField(2, "ts", new TimestampType())));

        List<ConnectorTableMetadata.Column> columns =
                PaimonTypeMapper.toPangrovaColumns(rowType);

        assertThat(columns).hasSize(3);
        assertColumn(columns.get(0), "id", PangrovaType.INTEGER, 0);
        assertColumn(columns.get(1), "name", PangrovaType.VARCHAR, 1);
        assertColumn(columns.get(2), "ts", PangrovaType.TIMESTAMP, 2);
    }

    @Test
    void toPangrovaColumns_emptyRowType() {
        RowType rowType = RowType.of();
        assertThat(PaimonTypeMapper.toPangrovaColumns(rowType)).isEmpty();
    }

    private void assertColumn(
            ConnectorTableMetadata.Column col,
            String name, PangrovaType type, int fieldId) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
        assertThat(col.fieldId()).hasValue(fieldId);
    }

    static Stream<Arguments> primitiveTypeMappings() {
        return Stream.of(
                Arguments.of(new BooleanType(), PangrovaType.BOOLEAN),
                Arguments.of(new TinyIntType(), PangrovaType.TINYINT),
                Arguments.of(new SmallIntType(), PangrovaType.SMALLINT),
                Arguments.of(new IntType(), PangrovaType.INTEGER),
                Arguments.of(new BigIntType(), PangrovaType.BIGINT),
                Arguments.of(new FloatType(), PangrovaType.FLOAT),
                Arguments.of(new DoubleType(), PangrovaType.DOUBLE),
                Arguments.of(new DecimalType(10, 2), PangrovaType.DECIMAL),
                Arguments.of(new DateType(), PangrovaType.DATE),
                Arguments.of(new TimeType(), PangrovaType.TIME),
                Arguments.of(new TimestampType(), PangrovaType.TIMESTAMP),
                Arguments.of(new LocalZonedTimestampType(), PangrovaType.TIMESTAMP_TZ),
                Arguments.of(new CharType(10), PangrovaType.VARCHAR),
                Arguments.of(new VarCharType(), PangrovaType.VARCHAR),
                Arguments.of(new BinaryType(16), PangrovaType.VARBINARY),
                Arguments.of(new VarBinaryType(), PangrovaType.VARBINARY),
                Arguments.of(new VariantType(), PangrovaType.VARIANT));
    }

    static Stream<Arguments> reverseTypeMappings() {
        return Stream.of(
                Arguments.of(PangrovaType.BOOLEAN, BooleanType.class),
                Arguments.of(PangrovaType.TINYINT, TinyIntType.class),
                Arguments.of(PangrovaType.SMALLINT, SmallIntType.class),
                Arguments.of(PangrovaType.INTEGER, IntType.class),
                Arguments.of(PangrovaType.BIGINT, BigIntType.class),
                Arguments.of(PangrovaType.FLOAT, FloatType.class),
                Arguments.of(PangrovaType.DOUBLE, DoubleType.class),
                Arguments.of(PangrovaType.DECIMAL, DecimalType.class),
                Arguments.of(PangrovaType.DATE, DateType.class),
                Arguments.of(PangrovaType.TIME, TimeType.class),
                Arguments.of(PangrovaType.TIMESTAMP, TimestampType.class),
                Arguments.of(PangrovaType.TIMESTAMP_TZ, LocalZonedTimestampType.class),
                Arguments.of(PangrovaType.VARCHAR, VarCharType.class),
                Arguments.of(PangrovaType.VARBINARY, VarBinaryType.class));
    }

    // --- Vector type conversion tests ---

    @Test
    void toPaimonVectorType_float() {
        VectorType vectorType = new VectorType(128, VectorType.ElementType.FLOAT);
        ArrayType result = PaimonTypeMapper.toPaimonVectorType(vectorType);

        assertThat(result).isInstanceOf(ArrayType.class);
        assertThat(result.getElementType()).isInstanceOf(FloatType.class);
    }

    @Test
    void toPaimonVectorType_double() {
        VectorType vectorType = new VectorType(384, VectorType.ElementType.DOUBLE);
        ArrayType result = PaimonTypeMapper.toPaimonVectorType(vectorType);

        assertThat(result).isInstanceOf(ArrayType.class);
        assertThat(result.getElementType()).isInstanceOf(DoubleType.class);
    }

    @Test
    void toPangrovaType_withVectorMetadata_returnsVector() {
        DataType arrayFloat = new ArrayType(new FloatType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.embedding", "768");

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayFloat, "embedding", props);

        assertThat(result).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void toPangrovaType_arrayFloatWithoutMetadata_returnsArray() {
        DataType arrayFloat = new ArrayType(new FloatType());

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayFloat, "embedding", Map.of());

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_arrayStringWithVectorMetadata_returnsArray() {
        DataType arrayString = new ArrayType(new VarCharType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.tags", "128");

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayString, "tags", props);

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_nullProperties_returnsNormalType() {
        DataType arrayFloat = new ArrayType(new FloatType());

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayFloat, "embedding", null);

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_nullColumnName_returnsNormalType() {
        DataType arrayFloat = new ArrayType(new FloatType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.embedding", "768");

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayFloat, null, props);

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toParameterizedType_withVectorMetadata_returnsVectorType() {
        DataType arrayFloat = new ArrayType(new FloatType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.embedding", "768");

        org.pangrova.spi.connector.data.type.Type result =
                PaimonTypeMapper.toParameterizedType(arrayFloat, "embedding", props);

        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(768);
        assertThat(vt.elementType()).isEqualTo(VectorType.ElementType.FLOAT);
    }

    @Test
    void toParameterizedType_doubleVectorMetadata_returnsDoubleElementType() {
        DataType arrayDouble = new ArrayType(new DoubleType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.emb", "384");

        org.pangrova.spi.connector.data.type.Type result =
                PaimonTypeMapper.toParameterizedType(arrayDouble, "emb", props);

        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorType.ElementType.DOUBLE);
    }

    @Test
    void toParameterizedType_invalidDimension_throws() {
        DataType arrayFloat = new ArrayType(new FloatType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.emb", "notANumber");

        assertThatThrownBy(() -> PaimonTypeMapper.toParameterizedType(arrayFloat, "emb", props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid vector dimension")
                .hasMessageContaining("emb");
    }

    @Test
    void vectorDimensionKey_format() {
        assertThat(PaimonTypeMapper.vectorDimensionKey("embedding"))
                .isEqualTo("pangrova.vector.dimension.embedding");
    }

    @Test
    void toPangrovaType_doubleArrayWithVectorMetadata_returnsVector() {
        DataType arrayDouble = new ArrayType(new DoubleType());
        Map<String, String> props = Map.of("pangrova.vector.dimension.emb", "256");

        PangrovaType result = PaimonTypeMapper.toPangrovaType(arrayDouble, "emb", props);

        assertThat(result).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void toPangrovaType_nonArrayWithVectorMetadata_returnsOriginalType() {
        DataType intType = new IntType();
        Map<String, String> props = Map.of("pangrova.vector.dimension.col", "128");

        PangrovaType result = PaimonTypeMapper.toPangrovaType(intType, "col", props);

        assertThat(result).isEqualTo(PangrovaType.INTEGER);
    }
}
