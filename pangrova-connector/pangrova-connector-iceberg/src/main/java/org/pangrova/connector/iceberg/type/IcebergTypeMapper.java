package org.pangrova.connector.iceberg.type;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.RowField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Bidirectional type mapper between Apache Iceberg types and Pangrova's
 * canonical type system. Acts as the anti-corruption layer preventing
 * Iceberg SDK types from leaking into core packages.
 */
public final class IcebergTypeMapper {

    /**
     * Table property key prefix for vector column dimension metadata.
     * Format: {@code pangrova.vector.dimension.<columnName>=<dimension>}.
     * Presence of this property marks a {@code list(float/double)} column as a vector column.
     */
    public static final String VECTOR_DIMENSION_KEY_PREFIX = "pangrova.vector.dimension.";

    /** Default precision when mapping a precision-less Pangrova DECIMAL to Iceberg. */
    private static final int DEFAULT_DECIMAL_PRECISION = 38;

    /** Default scale when mapping a scale-less Pangrova DECIMAL to Iceberg. */
    private static final int DEFAULT_DECIMAL_SCALE = 18;

    private IcebergTypeMapper() {}

    /**
     * Converts an Iceberg type to the corresponding PangrovaType.
     *
     * @param icebergType the Iceberg type to convert
     * @return the corresponding PangrovaType
     * @throws IllegalArgumentException if the type is unsupported
     */
    public static PangrovaType toPangrovaType(Type icebergType) {
        PangrovaType special = mapSpecialType(icebergType);
        if (special != null) {
            return special;
        }
        if (icebergType.isPrimitiveType()) {
            return mapPrimitiveToPangrova(icebergType.asPrimitiveType());
        }
        return mapComplexToPangrova(icebergType.asNestedType());
    }

    /**
     * Converts an PangrovaType to the corresponding Iceberg type (write path).
     *
     * @param pangrovaType the PangrovaType to convert
     * @return the corresponding Iceberg type
     * @throws IllegalArgumentException if the type is unsupported
     */
    public static Type toIcebergType(PangrovaType pangrovaType) {
        return switch (pangrovaType) {
            case BOOLEAN -> Types.BooleanType.get();
            case TINYINT, SMALLINT -> Types.IntegerType.get();
            case INTEGER -> Types.IntegerType.get();
            case BIGINT -> Types.LongType.get();
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case DECIMAL -> Types.DecimalType.of(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            case DATE -> Types.DateType.get();
            case TIME -> Types.TimeType.get();
            case TIMESTAMP -> Types.TimestampType.withoutZone();
            case TIMESTAMP_TZ -> Types.TimestampType.withZone();
            case TIMESTAMP_NS -> Types.TimestampNanoType.withoutZone();
            case VARCHAR -> Types.StringType.get();
            case UUID -> Types.UUIDType.get();
            case FIXED -> Types.FixedType.ofLength(16);
            case VARBINARY -> Types.BinaryType.get();
            case VARIANT -> Types.VariantType.get();
            case GEOMETRY -> Types.GeometryType.crs84();
            case GEOGRAPHY -> Types.GeographyType.crs84();
            default -> throw unsupported("PangrovaType", pangrovaType.name());
        };
    }

    /**
     * Converts a {@link VectorType} to the corresponding Iceberg {@code list(float/double)} type.
     *
     * <p>Iceberg has no native fixed-size list type. Vectors are stored as
     * {@code list(float)} or {@code list(double)}, with the dimension recorded
     * in table properties via {@link #VECTOR_DIMENSION_KEY_PREFIX}.</p>
     *
     * @param vectorType the Pangrova vector type
     * @param elementFieldId the Iceberg field ID for the list element
     * @return the corresponding Iceberg ListType
     */
    public static Types.ListType toIcebergVectorType(VectorType vectorType, int elementFieldId) {
        Type.PrimitiveType elementType = vectorType.elementType() == VectorType.ElementType.DOUBLE
                ? Types.DoubleType.get()
                : Types.FloatType.get();
        return Types.ListType.ofRequired(elementFieldId, elementType);
    }

    /**
     * Builds the table property key for a vector column's dimension metadata.
     *
     * @param columnName the vector column name
     * @return the property key, e.g. {@code "pangrova.vector.dimension.embedding"}
     */
    public static String vectorDimensionKey(String columnName) {
        return VECTOR_DIMENSION_KEY_PREFIX + columnName;
    }

    /**
     * Converts an Iceberg type to an Pangrova type, using table properties to detect
     * vector columns. A {@code list(float/double)} column with a matching
     * {@code pangrova.vector.dimension.<columnName>} property is mapped to
     * {@link PangrovaType#VECTOR} instead of {@link PangrovaType#ARRAY}.
     *
     * @param icebergType     the Iceberg type
     * @param columnName      the column name (for vector metadata lookup)
     * @param tableProperties the Iceberg table properties
     * @return the corresponding PangrovaType
     */
    public static PangrovaType toPangrovaType(Type icebergType, String columnName,
                                          Map<String, String> tableProperties) {
        if (isVectorColumn(icebergType, columnName, tableProperties)) {
            return PangrovaType.VECTOR;
        }
        return toPangrovaType(icebergType);
    }

    /**
     * Converts an Iceberg type to a parameterized Pangrova {@link org.pangrova.spi.connector.data.type.Type},
     * using table properties to detect vector columns. A {@code list(float/double)} column
     * with a matching dimension property is mapped to {@link VectorType}.
     *
     * @param icebergType     the Iceberg type
     * @param columnName      the column name (for vector metadata lookup)
     * @param tableProperties the Iceberg table properties
     * @return the corresponding parameterized Type
     */
    public static org.pangrova.spi.connector.data.type.Type toParameterizedType(
            Type icebergType, String columnName, Map<String, String> tableProperties) {
        if (isVectorColumn(icebergType, columnName, tableProperties)) {
            return parseVectorFromMetadata(icebergType, columnName, tableProperties);
        }
        return toParameterizedType(icebergType);
    }

    /**
     * Checks whether an Iceberg column represents a vector type based on its
     * Iceberg type (must be {@code list(float)} or {@code list(double)}) and
     * the presence of a dimension property in table metadata.
     */
    private static boolean isVectorColumn(Type icebergType, String columnName,
                                          Map<String, String> tableProperties) {
        if (tableProperties == null || columnName == null) {
            return false;
        }
        String dimValue = tableProperties.get(vectorDimensionKey(columnName));
        if (dimValue == null) {
            return false;
        }
        return isFloatOrDoubleList(icebergType);
    }

    private static boolean isFloatOrDoubleList(Type icebergType) {
        if (icebergType.typeId() != Type.TypeID.LIST) {
            return false;
        }
        Types.ListType list = (Types.ListType) icebergType.asNestedType();
        Type.TypeID elementId = list.elementType().typeId();
        return elementId == Type.TypeID.FLOAT || elementId == Type.TypeID.DOUBLE;
    }

    private static VectorType parseVectorFromMetadata(Type icebergType, String columnName,
                                                      Map<String, String> tableProperties) {
        String dimValue = tableProperties.get(vectorDimensionKey(columnName));
        int dimension;
        try {
            dimension = Integer.parseInt(dimValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid vector dimension for column '" + columnName + "': " + dimValue, e);
        }
        Types.ListType list = (Types.ListType) icebergType.asNestedType();
        VectorType.ElementType elementType = list.elementType().typeId() == Type.TypeID.DOUBLE
                ? VectorType.ElementType.DOUBLE
                : VectorType.ElementType.FLOAT;
        return new VectorType(dimension, elementType);
    }

    /**
     * Converts an Iceberg Schema to a list of typed columns,
     * preserving Iceberg field IDs for schema evolution.
     *
     * @param schema the Iceberg schema
     * @return ordered list of typed columns with field IDs
     */
    public static List<ConnectorTableMetadata.Column> toPangrovaColumns(Schema schema) {
        return schema.columns().stream()
                .map(IcebergTypeMapper::toColumn)
                .toList();
    }

    /**
     * Converts an Iceberg Schema to typed columns, recognizing VECTOR columns
     * via table properties (dimension metadata stored during CREATE TABLE).
     */
    public static List<ConnectorTableMetadata.Column> toPangrovaColumns(
            Schema schema, Map<String, String> tableProperties) {
        return schema.columns().stream()
                .map(field -> toColumnWithProperties(field, tableProperties))
                .toList();
    }

    /**
     * Converts an Iceberg type to the parameterized {@link org.pangrova.spi.connector.data.Type}.
     * For DECIMAL, ARRAY, MAP, and STRUCT, preserves full parameter information.
     * For simple types, bridges via {@link PangrovaTypeMapping}.
     *
     * @param icebergType the Iceberg type to convert
     * @return the corresponding parameterized Type
     */
    public static org.pangrova.spi.connector.data.type.Type toParameterizedType(Type icebergType) {
        PangrovaType special = mapSpecialType(icebergType);
        if (special != null) {
            return PangrovaTypeMapping.toType(special);
        }
        if (icebergType.isPrimitiveType()) {
            return mapPrimitiveToParameterized(icebergType.asPrimitiveType());
        }
        return mapComplexToParameterized(icebergType.asNestedType());
    }

    private static org.pangrova.spi.connector.data.type.Type mapPrimitiveToParameterized(
            Type.PrimitiveType primitive) {
        if (primitive.typeId() == Type.TypeID.DECIMAL) {
            Types.DecimalType dt = (Types.DecimalType) primitive;
            return new DecimalType(dt.precision(), dt.scale());
        }
        return PangrovaTypeMapping.toType(mapPrimitiveToPangrova(primitive));
    }

    private static org.pangrova.spi.connector.data.type.Type mapComplexToParameterized(
            Type.NestedType nested) {
        return switch (nested.typeId()) {
            case LIST -> {
                Types.ListType list = (Types.ListType) nested;
                yield new ArrayType(toParameterizedType(list.elementType()));
            }
            case MAP -> {
                Types.MapType map = (Types.MapType) nested;
                yield new MapType(
                        toParameterizedType(map.keyType()),
                        toParameterizedType(map.valueType()));
            }
            case STRUCT -> {
                Types.StructType struct = (Types.StructType) nested;
                var fields = struct.fields().stream()
                        .map(f -> new RowField(f.name(), toParameterizedType(f.type())))
                        .toList();
                yield new RowType(fields);
            }
            default -> throw unsupported("Iceberg nested", nested.typeId().name());
        };
    }

    private static ConnectorTableMetadata.Column toColumn(Types.NestedField field) {
        PangrovaType type = toPangrovaType(field.type());
        org.pangrova.spi.connector.data.type.Type paramType = toParameterizedType(field.type());
        return new ConnectorTableMetadata.Column(
                field.name(), type, OptionalInt.of(field.fieldId()), paramType);
    }

    private static ConnectorTableMetadata.Column toColumnWithProperties(
            Types.NestedField field, Map<String, String> tableProperties) {
        PangrovaType type = toPangrovaType(field.type(), field.name(), tableProperties);
        org.pangrova.spi.connector.data.type.Type paramType =
                toParameterizedType(field.type(), field.name(), tableProperties);
        return new ConnectorTableMetadata.Column(
                field.name(), type, OptionalInt.of(field.fieldId()), paramType);
    }

    private static PangrovaType mapPrimitiveToPangrova(Type.PrimitiveType primitive) {
        return switch (primitive.typeId()) {
            case BOOLEAN -> PangrovaType.BOOLEAN;
            case INTEGER -> PangrovaType.INTEGER;
            case LONG -> PangrovaType.BIGINT;
            case FLOAT -> PangrovaType.FLOAT;
            case DOUBLE -> PangrovaType.DOUBLE;
            case DECIMAL -> PangrovaType.DECIMAL;
            case DATE -> PangrovaType.DATE;
            case TIME -> PangrovaType.TIME;
            case TIMESTAMP -> mapTimestamp((Types.TimestampType) primitive);
            case TIMESTAMP_NANO -> PangrovaType.TIMESTAMP_NS;
            case STRING -> PangrovaType.VARCHAR;
            case UUID -> PangrovaType.UUID;
            case FIXED -> PangrovaType.FIXED;
            case BINARY -> PangrovaType.VARBINARY;
            case VARIANT -> PangrovaType.VARIANT;
            case GEOMETRY -> PangrovaType.GEOMETRY;
            case GEOGRAPHY -> PangrovaType.GEOGRAPHY;
            case UNKNOWN -> PangrovaType.UNKNOWN;
            default -> throw unsupported("Iceberg primitive", primitive.typeId().name());
        };
    }

    private static PangrovaType mapTimestamp(Types.TimestampType ts) {
        return ts.shouldAdjustToUTC() ? PangrovaType.TIMESTAMP_TZ : PangrovaType.TIMESTAMP;
    }

    private static PangrovaType mapComplexToPangrova(Type.NestedType nested) {
        return switch (nested.typeId()) {
            case STRUCT -> PangrovaType.STRUCT;
            case LIST -> PangrovaType.ARRAY;
            case MAP -> PangrovaType.MAP;
            default -> throw unsupported("Iceberg nested", nested.typeId().name());
        };
    }

    /**
     * Maps Iceberg v3 special types that are neither primitive nor nested
     * in the Iceberg 1.10.x type hierarchy. Returns null if the type is
     * not a special type, allowing the caller to fall through to
     * primitive/nested dispatch.
     */
    private static PangrovaType mapSpecialType(Type icebergType) {
        return switch (icebergType.typeId()) {
            case VARIANT -> PangrovaType.VARIANT;
            case GEOMETRY -> PangrovaType.GEOMETRY;
            case GEOGRAPHY -> PangrovaType.GEOGRAPHY;
            case UNKNOWN -> PangrovaType.UNKNOWN;
            default -> null;
        };
    }

    /**
     * Converts a parameterized RowType (STRUCT) to an Iceberg StructType for the DDL/write path.
     * Recursively handles nested complex types (STRUCT, ARRAY, MAP).
     */
    public static Types.StructType toIcebergTypeFromParameterized(
            RowType rowType, int startFieldId) {
        List<Types.NestedField> fields = new java.util.ArrayList<>();
        for (int i = 0; i < rowType.fields().size(); i++) {
            RowField rf = rowType.fields().get(i);
            int fieldId = startFieldId + i;
            Type icebergFieldType = toIcebergTypeFromParameterizedType(rf.type(), fieldId * 100);
            fields.add(Types.NestedField.optional(fieldId, rf.name(), icebergFieldType));
        }
        return Types.StructType.of(fields);
    }

    /**
     * Converts a parameterized type to Iceberg Type, recursively handling STRUCT/ARRAY/MAP.
     */
    public static Type toIcebergTypeFromParameterizedType(
            org.pangrova.spi.connector.data.type.Type paramType, int baseFieldId) {
        if (paramType instanceof RowType rowType) {
            return toIcebergTypeFromParameterized(rowType, baseFieldId);
        } else if (paramType instanceof ArrayType arrayType) {
            Type elementIceberg = toIcebergTypeFromParameterizedType(arrayType.elementType(), baseFieldId + 1);
            return Types.ListType.ofOptional(baseFieldId, elementIceberg);
        } else if (paramType instanceof MapType mapType) {
            Type keyIceberg = toIcebergTypeFromParameterizedType(mapType.keyType(), baseFieldId + 1);
            Type valIceberg = toIcebergTypeFromParameterizedType(mapType.valueType(), baseFieldId + 2);
            return Types.MapType.ofOptional(baseFieldId, baseFieldId + 1000, keyIceberg, valIceberg);
        }
        PangrovaType at = PangrovaTypeMapping.toPangrovaType(paramType);
        return toIcebergType(at);
    }

    private static IllegalArgumentException unsupported(String domain, String typeName) {
        return new IllegalArgumentException(
                "Unsupported " + domain + " type: " + typeName);
    }
}
