package org.pangrova.connector.paimon.type;

import org.pangrova.types.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
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
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void toPangrovaColumns_preservesActualDecimalPrecisionAndScale(boolean withProperties) {
        var schema = new RowType(List.of(
                new DataField(17, "amount", new DecimalType(12, 3)),
                new DataField(29, "wide", new DecimalType(38, 18))));

        var columns = columns(schema, withProperties);

        assertThat(columns).hasSize(2);
        assertColumn(columns.get(0), "amount", PangrovaType.DECIMAL, 17);
        assertColumn(columns.get(1), "wide", PangrovaType.DECIMAL, 29);
        assertThat(columns.get(0).parameterizedType())
                .isEqualTo(new org.pangrova.types.primitive.DecimalType(12, 3));
        assertThat(columns.get(1).parameterizedType())
                .isEqualTo(new org.pangrova.types.primitive.DecimalType(38, 18));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void toPangrovaColumns_preservesOrderedNestedTypeParameters(boolean withProperties) {
        var nested = new RowType(List.of(
                new DataField(7, "history", new ArrayType(new DecimalType(21, 7))),
                new DataField(11, "prices", new MapType(new VarCharType(27), new DecimalType(12, 4))),
                new DataField(15, "code", new CharType(7))));
        var schema = new RowType(List.of(new DataField(31, "details", nested)));
        var expected = new org.pangrova.types.RowType(List.of(
                new org.pangrova.types.RowField("history",
                        new org.pangrova.types.ArrayType(
                                new org.pangrova.types.primitive.DecimalType(21, 7))),
                new org.pangrova.types.RowField("prices",
                        new org.pangrova.types.MapType(
                                new org.pangrova.types.primitive.VarcharType(27),
                                new org.pangrova.types.primitive.DecimalType(12, 4))),
                new org.pangrova.types.RowField("code",
                        new org.pangrova.types.primitive.CharType(7))));

        var columns = columns(schema, withProperties);

        assertThat(columns).hasSize(1);
        assertColumn(columns.get(0), "details", PangrovaType.STRUCT, 31);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(expected);
        assertThat(columns.get(0).parameterizedType().getTypeSignature()).isEqualTo(expected.getTypeSignature());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void toPangrovaColumns_preservesSdkVectorParametersAndNonFloatingElements(boolean withProperties) {
        var schema = new RowType(List.of(
                new DataField(3, "single", DataTypes.VECTOR(3, DataTypes.FLOAT())),
                new DataField(9, "doubled", DataTypes.VECTOR(9, DataTypes.DOUBLE())),
                new DataField(13, "integers", DataTypes.VECTOR(4, DataTypes.INT()))));

        var columns = columns(schema, withProperties);

        assertThat(columns).hasSize(3);
        assertColumn(columns.get(0), "single", PangrovaType.VECTOR, 3);
        assertColumn(columns.get(1), "doubled", PangrovaType.VECTOR, 9);
        assertColumn(columns.get(2), "integers", PangrovaType.ARRAY, 13);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(new VectorType(3, VectorType.ElementType.FLOAT));
        assertThat(columns.get(1).parameterizedType()).isEqualTo(new VectorType(9, VectorType.ElementType.DOUBLE));
        assertThat(columns.get(2).parameterizedType()).isEqualTo(new org.pangrova.types.ArrayType(
                org.pangrova.types.primitive.IntegerType.INSTANCE));
    }

    @Test
    void toPangrovaColumns_usesTheSameVectorPropertiesForEnumAndParameterizedType() {
        var schema = new RowType(List.of(
                new DataField(5, "single", new ArrayType(new FloatType())),
                new DataField(19, "doubled", new ArrayType(new DoubleType()))));
        var properties = Map.of(PaimonTypeMapper.vectorDimensionKey("single"), "128",
                PaimonTypeMapper.vectorDimensionKey("doubled"), "384");

        var columns = PaimonTypeMapper.toPangrovaColumns(schema, properties);

        assertColumn(columns.get(0), "single", PangrovaType.VECTOR, 5);
        assertColumn(columns.get(1), "doubled", PangrovaType.VECTOR, 19);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(new VectorType(128, VectorType.ElementType.FLOAT));
        assertThat(columns.get(1).parameterizedType()).isEqualTo(new VectorType(384, VectorType.ElementType.DOUBLE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"notANumber", "0"})
    void toPangrovaColumns_rejectsInvalidVectorPropertiesWithoutAnEnumOnlyResult(String dimension) {
        var schema = new RowType(List.of(new DataField(5, "embedding", new ArrayType(new FloatType()))));
        var properties = Map.of(PaimonTypeMapper.vectorDimensionKey("embedding"), dimension);

        assertThatThrownBy(() -> PaimonTypeMapper.toPangrovaColumns(schema, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension");
    }

    @Test
    void markedVariantUsesCanonicalVariantStorageInBothPropertyAwareEntrypoints() {
        var storage = new VarCharType(123);
        var properties = Map.of(PaimonTypeMapper.VARIANT_COLUMN_KEY_PREFIX + "document", "true");
        var schema = new RowType(List.of(new DataField(23, "document", storage),
                new DataField(31, "plain", new VarCharType(41))));
        var expected = org.pangrova.types.primitive.VarcharType.UNBOUNDED;

        assertThat(PaimonTypeMapper.toPangrovaType(storage, "document", properties)).isEqualTo(PangrovaType.VARIANT);
        assertThat(PaimonTypeMapper.toParameterizedType(storage, "document", properties)).isEqualTo(expected);
        assertThat(PaimonTypeMapper.toParameterizedType(new VariantType())).isEqualTo(expected);
        var columns = PaimonTypeMapper.toPangrovaColumns(schema, properties);
        assertThat(columns).hasSize(2);
        assertColumn(columns.get(0), "document", PangrovaType.VARIANT, 23);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(expected);
        assertColumn(columns.get(1), "plain", PangrovaType.VARCHAR, 31);
        assertThat(columns.get(1).parameterizedType())
                .isEqualTo(new org.pangrova.types.primitive.VarcharType(41));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryVarcharRetainsItsSdkLengthWithoutAVariantMarker(boolean withProperties) {
        var storage = new VarCharType(123);
        var schema = new RowType(List.of(new DataField(23, "document", storage)));
        var expected = new org.pangrova.types.primitive.VarcharType(123);

        assertThat(PaimonTypeMapper.toParameterizedType(storage)).isEqualTo(expected);
        assertThat(PaimonTypeMapper.toParameterizedType(storage, "document", Map.of())).isEqualTo(expected);
        var columns = columns(schema, withProperties);
        assertColumn(columns.get(0), "document", PangrovaType.VARCHAR, 23);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "TRUE"})
    void nonVariantPropertyValuesDoNotDiscardTheSdkVarcharLength(String marker) {
        var storage = new VarCharType(123);
        var properties = Map.of(PaimonTypeMapper.VARIANT_COLUMN_KEY_PREFIX + "document", marker);
        var schema = new RowType(List.of(new DataField(23, "document", storage)));
        var expected = new org.pangrova.types.primitive.VarcharType(123);

        assertThat(PaimonTypeMapper.toPangrovaType(storage, "document", properties)).isEqualTo(PangrovaType.VARCHAR);
        assertThat(PaimonTypeMapper.toParameterizedType(storage, "document", properties)).isEqualTo(expected);
        var columns = PaimonTypeMapper.toPangrovaColumns(schema, properties);
        assertColumn(columns.get(0), "document", PangrovaType.VARCHAR, 23);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(expected);
    }

    private static List<ConnectorTableMetadata.Column> columns(RowType schema, boolean withProperties) {
        return withProperties ? PaimonTypeMapper.toPangrovaColumns(schema, Map.of())
                : PaimonTypeMapper.toPangrovaColumns(schema);
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

        org.pangrova.types.Type result =
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

        org.pangrova.types.Type result =
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
