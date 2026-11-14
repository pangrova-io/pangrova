package org.pangrova.connector.iceberg.type;

import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.NullOnlyType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
 * Unit tests for IcebergTypeMapper — verifies bidirectional type mapping
 * between Iceberg types and PangrovaType, plus schema-to-column conversion.
 */
class IcebergTypeMapperTest {

    @Test
    void uuidSdkSchemasRetainScalarAndNestedTypeIdentity() {
        var uuid = org.pangrova.types.primitive.UuidType.INSTANCE;
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.UUIDType.get()),
                Types.NestedField.optional(2, "ids", Types.ListType.ofOptional(3, Types.UUIDType.get())),
                Types.NestedField.optional(4, "lookup", Types.MapType.ofOptional(
                        5, 6, Types.UUIDType.get(), Types.UUIDType.get())),
                Types.NestedField.optional(7, "nested", Types.StructType.of(
                        Types.NestedField.optional(8, "id", Types.UUIDType.get()))));
        var columns = IcebergTypeMapper.toPangrovaColumns(schema);
        assertThat(columns.get(0).type()).isEqualTo(PangrovaType.UUID);
        assertThat(columns.get(0).parameterizedType()).isEqualTo(uuid);
        assertThat(columns.get(1).parameterizedType())
                .isEqualTo(new org.pangrova.types.ArrayType(uuid));
        assertThat(columns.get(2).parameterizedType())
                .isEqualTo(new org.pangrova.types.MapType(uuid, uuid));
        assertThat(columns.get(3).parameterizedType())
                .isEqualTo(new org.pangrova.types.RowType(List.of(
                        new org.pangrova.types.RowField("id", uuid))));
    }

    @Test
    void icebergV3UnknownHasAnExplicitNullOnlyDeclaration() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "future", Types.UnknownType.get()));
        ConnectorTableMetadata.Column column = IcebergTypeMapper.toPangrovaColumns(schema).get(0);
        assertThat(column.type()).isEqualTo(PangrovaType.UNKNOWN);
        assertThat(column.parameterizedType()).isEqualTo(NullOnlyType.INSTANCE);
    }

    @Test
    void nestedUnknownFailsClosedUntilNativeCanSynthesizeNestedNulls() {
        Type nested = Types.StructType.of(
                Types.NestedField.optional(2, "future", Types.UnknownType.get()));
        assertThatThrownBy(() -> IcebergTypeMapper.toParameterizedType(nested))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recursive null-only Native projection");
    }

    @Test
    void timezoneNanoTimestampFailsClosedInsteadOfLosingTimezone() {
        Type timestamp = Types.TimestampNanoType.withZone();
        assertThatThrownBy(() -> IcebergTypeMapper.toPangrovaType(timestamp))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision-aware timestamp");
        assertThatThrownBy(() -> IcebergTypeMapper.toParameterizedType(timestamp))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision-aware timestamp");
        Schema schema = new Schema(Types.NestedField.optional(1, "event_time", timestamp));
        assertThatThrownBy(() -> IcebergTypeMapper.toPangrovaColumns(schema))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision-aware timestamp");
    }

    @Test
    void nestedTimezoneNanoTimestampAlsoFailsClosed() {
        Type nested = Types.ListType.ofOptional(2, Types.TimestampNanoType.withZone());
        assertThatThrownBy(() -> IcebergTypeMapper.toParameterizedType(nested))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision-aware timestamp");
        Schema schema = new Schema(Types.NestedField.optional(1, "events", nested));
        assertThatThrownBy(() -> IcebergTypeMapper.toPangrovaColumns(schema))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("precision-aware timestamp");
    }

    @ParameterizedTest(name = "Iceberg {0} -> PangrovaType.{1}")
    @MethodSource("primitiveTypeMappings")
    void toPangrovaType_primitives(Type icebergType, PangrovaType expected) {
        assertThat(IcebergTypeMapper.toPangrovaType(icebergType)).isEqualTo(expected);
    }

    @Test
    void toPangrovaType_struct() {
        Type struct = Types.StructType.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        assertThat(IcebergTypeMapper.toPangrovaType(struct)).isEqualTo(PangrovaType.STRUCT);
    }

    @Test
    void toPangrovaType_list() {
        Type list = Types.ListType.ofRequired(1, Types.StringType.get());
        assertThat(IcebergTypeMapper.toPangrovaType(list)).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_map() {
        Type map = Types.MapType.ofRequired(
                1, 2, Types.StringType.get(), Types.LongType.get());
        assertThat(IcebergTypeMapper.toPangrovaType(map)).isEqualTo(PangrovaType.MAP);
    }

    @ParameterizedTest(name = "PangrovaType.{0} -> Iceberg")
    @MethodSource("reverseTypeMappings")
    void toIcebergType_primitives(PangrovaType pangrovaType, Type.TypeID expectedTypeId) {
        Type result = IcebergTypeMapper.toIcebergType(pangrovaType);
        assertThat(result.typeId()).isEqualTo(expectedTypeId);
    }

    @Test
    void toIcebergType_unsupported_throws() {
        assertThatThrownBy(() -> IcebergTypeMapper.toIcebergType(PangrovaType.STRUCT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported PangrovaType type: STRUCT");
    }

    @Test
    void toIcebergType_tinyint_promotesToInteger() {
        Type result = IcebergTypeMapper.toIcebergType(PangrovaType.TINYINT);
        assertThat(result.typeId()).isEqualTo(Type.TypeID.INTEGER);
    }

    @Test
    void toIcebergType_smallint_promotesToInteger() {
        Type result = IcebergTypeMapper.toIcebergType(PangrovaType.SMALLINT);
        assertThat(result.typeId()).isEqualTo(Type.TypeID.INTEGER);
    }

    @Test
    void toPangrovaColumns_preservesFieldIds() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "ts", Types.TimestampType.withZone()));

        List<ConnectorTableMetadata.Column> columns =
                IcebergTypeMapper.toPangrovaColumns(schema);

        assertThat(columns).hasSize(3);
        assertColumn(columns.get(0), "id", PangrovaType.INTEGER, 1);
        assertColumn(columns.get(1), "name", PangrovaType.VARCHAR, 2);
        assertColumn(columns.get(2), "ts", PangrovaType.TIMESTAMP_TZ, 3);
    }

    @Test
    void toPangrovaColumns_emptySchema() {
        Schema schema = new Schema();
        assertThat(IcebergTypeMapper.toPangrovaColumns(schema)).isEmpty();
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
                Arguments.of(Types.BooleanType.get(), PangrovaType.BOOLEAN),
                Arguments.of(Types.IntegerType.get(), PangrovaType.INTEGER),
                Arguments.of(Types.LongType.get(), PangrovaType.BIGINT),
                Arguments.of(Types.FloatType.get(), PangrovaType.FLOAT),
                Arguments.of(Types.DoubleType.get(), PangrovaType.DOUBLE),
                Arguments.of(Types.DecimalType.of(10, 2), PangrovaType.DECIMAL),
                Arguments.of(Types.DateType.get(), PangrovaType.DATE),
                Arguments.of(Types.TimeType.get(), PangrovaType.TIME),
                Arguments.of(Types.TimestampType.withoutZone(), PangrovaType.TIMESTAMP),
                Arguments.of(Types.TimestampType.withZone(), PangrovaType.TIMESTAMP_TZ),
                Arguments.of(Types.StringType.get(), PangrovaType.VARCHAR),
                Arguments.of(Types.UUIDType.get(), PangrovaType.UUID),
                Arguments.of(Types.FixedType.ofLength(16), PangrovaType.FIXED),
                Arguments.of(Types.BinaryType.get(), PangrovaType.VARBINARY),
                Arguments.of(Types.VariantType.get(), PangrovaType.VARIANT),
                Arguments.of(Types.GeometryType.crs84(), PangrovaType.GEOMETRY),
                Arguments.of(Types.GeographyType.crs84(), PangrovaType.GEOGRAPHY),
                Arguments.of(Types.TimestampNanoType.withoutZone(), PangrovaType.TIMESTAMP_NS));
    }

    static Stream<Arguments> reverseTypeMappings() {
        return Stream.of(
                Arguments.of(PangrovaType.BOOLEAN, Type.TypeID.BOOLEAN),
                Arguments.of(PangrovaType.INTEGER, Type.TypeID.INTEGER),
                Arguments.of(PangrovaType.BIGINT, Type.TypeID.LONG),
                Arguments.of(PangrovaType.FLOAT, Type.TypeID.FLOAT),
                Arguments.of(PangrovaType.DOUBLE, Type.TypeID.DOUBLE),
                Arguments.of(PangrovaType.DECIMAL, Type.TypeID.DECIMAL),
                Arguments.of(PangrovaType.DATE, Type.TypeID.DATE),
                Arguments.of(PangrovaType.TIME, Type.TypeID.TIME),
                Arguments.of(PangrovaType.TIMESTAMP, Type.TypeID.TIMESTAMP),
                Arguments.of(PangrovaType.TIMESTAMP_TZ, Type.TypeID.TIMESTAMP),
                Arguments.of(PangrovaType.VARCHAR, Type.TypeID.STRING),
                Arguments.of(PangrovaType.UUID, Type.TypeID.UUID),
                Arguments.of(PangrovaType.FIXED, Type.TypeID.FIXED),
                Arguments.of(PangrovaType.VARBINARY, Type.TypeID.BINARY),
                Arguments.of(PangrovaType.VARIANT, Type.TypeID.VARIANT),
                Arguments.of(PangrovaType.GEOMETRY, Type.TypeID.GEOMETRY),
                Arguments.of(PangrovaType.GEOGRAPHY, Type.TypeID.GEOGRAPHY),
                Arguments.of(PangrovaType.TIMESTAMP_NS, Type.TypeID.TIMESTAMP_NANO));
    }

    // --- Vector type conversion tests ---

    @Test
    void toIcebergVectorType_float() {
        VectorType vectorType = new VectorType(128, VectorType.ElementType.FLOAT);
        Types.ListType result = IcebergTypeMapper.toIcebergVectorType(vectorType, 1);

        assertThat(result.typeId()).isEqualTo(Type.TypeID.LIST);
        assertThat(result.elementType().typeId()).isEqualTo(Type.TypeID.FLOAT);
    }

    @Test
    void toIcebergVectorType_double() {
        VectorType vectorType = new VectorType(384, VectorType.ElementType.DOUBLE);
        Types.ListType result = IcebergTypeMapper.toIcebergVectorType(vectorType, 2);

        assertThat(result.typeId()).isEqualTo(Type.TypeID.LIST);
        assertThat(result.elementType().typeId()).isEqualTo(Type.TypeID.DOUBLE);
    }

    @Test
    void toIcebergType_vector_throws() {
        assertThatThrownBy(() -> IcebergTypeMapper.toIcebergType(PangrovaType.VECTOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VECTOR");
    }

    @Test
    void toPangrovaType_withVectorMetadata_returnsVector() {
        Type listFloat = Types.ListType.ofRequired(1, Types.FloatType.get());
        Map<String, String> props = Map.of("pangrova.vector.dimension.embedding", "768");

        PangrovaType result = IcebergTypeMapper.toPangrovaType(listFloat, "embedding", props);

        assertThat(result).isEqualTo(PangrovaType.VECTOR);
    }

    @Test
    void toPangrovaType_listFloatWithoutMetadata_returnsArray() {
        Type listFloat = Types.ListType.ofRequired(1, Types.FloatType.get());

        PangrovaType result = IcebergTypeMapper.toPangrovaType(listFloat, "embedding", Map.of());

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toPangrovaType_listStringWithVectorMetadata_returnsArray() {
        Type listString = Types.ListType.ofRequired(1, Types.StringType.get());
        Map<String, String> props = Map.of("pangrova.vector.dimension.tags", "128");

        PangrovaType result = IcebergTypeMapper.toPangrovaType(listString, "tags", props);

        assertThat(result).isEqualTo(PangrovaType.ARRAY);
    }

    @Test
    void toParameterizedType_withVectorMetadata_returnsVectorType() {
        Type listFloat = Types.ListType.ofRequired(1, Types.FloatType.get());
        Map<String, String> props = Map.of("pangrova.vector.dimension.embedding", "768");

        org.pangrova.types.Type result =
                IcebergTypeMapper.toParameterizedType(listFloat, "embedding", props);

        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(768);
        assertThat(vt.elementType()).isEqualTo(VectorType.ElementType.FLOAT);
    }

    @Test
    void toParameterizedType_doubleVectorMetadata_returnsDoubleElementType() {
        Type listDouble = Types.ListType.ofRequired(1, Types.DoubleType.get());
        Map<String, String> props = Map.of("pangrova.vector.dimension.emb", "384");

        org.pangrova.types.Type result =
                IcebergTypeMapper.toParameterizedType(listDouble, "emb", props);

        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vt = (VectorType) result;
        assertThat(vt.dimension()).isEqualTo(384);
        assertThat(vt.elementType()).isEqualTo(VectorType.ElementType.DOUBLE);
    }

    @Test
    void vectorDimensionKey_format() {
        assertThat(IcebergTypeMapper.vectorDimensionKey("embedding"))
                .isEqualTo("pangrova.vector.dimension.embedding");
    }
}
