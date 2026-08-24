package org.pangrova.connector.paimon.type;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Bidirectional type mapper between Apache Paimon types and Pangrova's
 * canonical type system. Acts as the anti-corruption layer preventing
 * Paimon SDK types from leaking into core packages.
 */
public final class PaimonTypeMapper {

    /**
     * Table property key prefix for vector column dimension metadata.
     * Format: {@code pangrova.vector.dimension.<columnName>=<dimension>}.
     * Presence of this property marks a {@code list(float/double)} column as a vector column.
     */
    public static final String VECTOR_DIMENSION_KEY_PREFIX = "pangrova.vector.dimension.";

    /**
     * Table property key prefix for variant column metadata.
     * Format: {@code pangrova.variant.column.<columnName>=true}.
     * VARIANT columns are stored as VarCharType in Paimon (plain JSON strings)
     * because Velox cannot read Paimon's native binary variant Parquet format.
     */
    public static final String VARIANT_COLUMN_KEY_PREFIX = "pangrova.variant.column.";

    /** Default precision used when mapping DECIMAL to Paimon. */
    private static final int DEFAULT_DECIMAL_PRECISION = 38;

    /** Default scale used when mapping DECIMAL to Paimon. */
    private static final int DEFAULT_DECIMAL_SCALE = 18;

    /** Character length of the canonical UUID string representation. */
    private static final int UUID_VARCHAR_LENGTH = 36;

    private static final PaimonToPangrovaTypeVisitors.PangrovaTypeVisitor TO_PANGROVA =
            new PaimonToPangrovaTypeVisitors.PangrovaTypeVisitor();
    private static final PaimonToPangrovaTypeVisitors.ParameterizedTypeVisitor TO_PARAMETERIZED =
            new PaimonToPangrovaTypeVisitors.ParameterizedTypeVisitor();

    private PaimonTypeMapper() {}

    /**
     * Converts a Paimon DataType to the corresponding PangrovaType.
     */
    public static PangrovaType toPangrovaType(DataType paimonType) {
        return paimonType.accept(TO_PANGROVA);
    }

    /**
     * Converts a Paimon DataType to the parameterized {@link org.pangrova.spi.connector.data.Type}.
     * For DECIMAL, ARRAY, MAP, and ROW, preserves full parameter information.
     * For simple types, bridges via {@link PangrovaTypeMapping}.
     *
     * @param paimonType the Paimon type to convert
     * @return the corresponding parameterized Type
     */
    public static org.pangrova.spi.connector.data.type.Type toParameterizedType(DataType paimonType) {
        return paimonType.accept(TO_PARAMETERIZED);
    }

    /**
     * Converts a {@link VectorType} to the corresponding Paimon {@code ArrayType(float/double)}.
     *
     * <p>Paimon/ORC has no native fixed-size list type. Vectors are stored as
     * {@code array(float)} or {@code array(double)}, with the dimension recorded
     * in table properties via {@link #VECTOR_DIMENSION_KEY_PREFIX}.</p>
     *
     * @param vectorType the Pangrova vector type
     * @return the corresponding Paimon ArrayType
     */
    public static ArrayType toPaimonVectorType(VectorType vectorType) {
        DataType elementType = vectorType.elementType() == VectorType.ElementType.DOUBLE
                ? new DoubleType()
                : new FloatType();
        return new ArrayType(elementType);
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
     * Converts a Paimon type to an Pangrova type, using table properties to detect
     * vector columns. A {@code array(float/double)} column with a matching
     * {@code pangrova.vector.dimension.<columnName>} property is mapped to
     * {@link PangrovaType#VECTOR} instead of {@link PangrovaType#ARRAY}.
     *
     * @param paimonType      the Paimon type
     * @param columnName      the column name (for vector metadata lookup)
     * @param tableProperties the table properties
     * @return the corresponding PangrovaType
     */
    public static PangrovaType toPangrovaType(DataType paimonType, String columnName,
                                          Map<String, String> tableProperties) {
        if (isVariantColumn(paimonType, columnName, tableProperties)) {
            return PangrovaType.VARIANT;
        }
        if (isVectorColumn(paimonType, columnName, tableProperties)) {
            return PangrovaType.VECTOR;
        }
        return toPangrovaType(paimonType);
    }

    /**
     * Converts a Paimon type to a parameterized Pangrova {@link org.pangrova.spi.connector.data.type.Type},
     * using table properties to detect vector columns. A {@code array(float/double)} column
     * with a matching dimension property is mapped to {@link VectorType}.
     *
     * @param paimonType      the Paimon type
     * @param columnName      the column name (for vector metadata lookup)
     * @param tableProperties the table properties
     * @return the corresponding parameterized Type
     */
    public static org.pangrova.spi.connector.data.type.Type toParameterizedType(
            DataType paimonType, String columnName, Map<String, String> tableProperties) {
        if (isVectorColumn(paimonType, columnName, tableProperties)) {
            return parseVectorFromMetadata(paimonType, columnName, tableProperties);
        }
        return toParameterizedType(paimonType);
    }

    /**
     * Checks whether a Paimon column represents a vector type based on its
     * Paimon type (must be {@code array(float)} or {@code array(double)}) and
     * the presence of a dimension property in table metadata.
     */
    private static boolean isVariantColumn(DataType paimonType, String columnName,
                                           Map<String, String> tableProperties) {
        if (tableProperties == null || columnName == null) {
            return false;
        }
        return "true".equals(tableProperties.get(VARIANT_COLUMN_KEY_PREFIX + columnName))
                && paimonType instanceof VarCharType;
    }

    private static boolean isVectorColumn(DataType paimonType, String columnName,
                                          Map<String, String> tableProperties) {
        if (tableProperties == null || columnName == null) {
            return false;
        }
        String dimValue = tableProperties.get(vectorDimensionKey(columnName));
        if (dimValue == null) {
            return false;
        }
        return isFloatOrDoubleArray(paimonType);
    }

    private static boolean isFloatOrDoubleArray(DataType paimonType) {
        if (!(paimonType instanceof ArrayType arrayType)) {
            return false;
        }
        DataType elementType = arrayType.getElementType();
        return elementType instanceof FloatType || elementType instanceof DoubleType;
    }

    private static VectorType parseVectorFromMetadata(DataType paimonType, String columnName,
                                                      Map<String, String> tableProperties) {
        String dimValue = tableProperties.get(vectorDimensionKey(columnName));
        int dimension;
        try {
            dimension = Integer.parseInt(dimValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid vector dimension for column '" + columnName + "': " + dimValue, e);
        }
        ArrayType arrayType = (ArrayType) paimonType;
        VectorType.ElementType elementType = arrayType.getElementType() instanceof DoubleType
                ? VectorType.ElementType.DOUBLE
                : VectorType.ElementType.FLOAT;
        return new VectorType(dimension, elementType);
    }

    /**
     * Converts an PangrovaType to the corresponding Paimon DataType (write path).
     */
    public static DataType toPaimonType(PangrovaType pangrovaType) {
        return switch (pangrovaType) {
            case BOOLEAN -> new BooleanType();
            case TINYINT -> new TinyIntType();
            case SMALLINT -> new SmallIntType();
            case INTEGER -> new IntType();
            case BIGINT -> new BigIntType();
            case FLOAT -> new FloatType();
            case DOUBLE -> new DoubleType();
            case DECIMAL -> new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            case DATE -> new DateType();
            case TIME -> new TimeType();
            case TIMESTAMP -> new TimestampType();
            case TIMESTAMP_TZ -> new LocalZonedTimestampType();
            case VARCHAR -> new VarCharType(VarCharType.MAX_LENGTH);
            case UUID -> new VarCharType(UUID_VARCHAR_LENGTH);
            case FIXED -> new BinaryType(16);
            case VARBINARY -> new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case VARIANT -> new VarCharType(VarCharType.MAX_LENGTH);
            case GEOMETRY, GEOGRAPHY -> new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case TIMESTAMP_NS -> new TimestampType(9);
            default -> throw unsupported("PangrovaType", pangrovaType.name());
        };
    }

    /**
     * Converts a Paimon RowType to a list of typed columns.
     */
    public static List<ConnectorTableMetadata.Column> toPangrovaColumns(RowType rowType) {
        return rowType.getFields().stream()
                .map(PaimonTypeMapper::toColumn)
                .toList();
    }

    /**
     * Converts a Paimon RowType to a list of typed columns, using table properties
     * to detect variant columns stored as VarCharType.
     */
    public static List<ConnectorTableMetadata.Column> toPangrovaColumns(
            RowType rowType, Map<String, String> tableProperties) {
        return rowType.getFields().stream()
                .map(f -> toColumn(f, tableProperties))
                .toList();
    }

    private static ConnectorTableMetadata.Column toColumn(DataField field) {
        PangrovaType type = toPangrovaType(field.type());
        return new ConnectorTableMetadata.Column(
                field.name(), type, OptionalInt.of(field.id()));
    }

    private static ConnectorTableMetadata.Column toColumn(DataField field,
                                                           Map<String, String> tableProperties) {
        PangrovaType type = toPangrovaType(field.type(), field.name(), tableProperties);
        return new ConnectorTableMetadata.Column(
                field.name(), type, OptionalInt.of(field.id()));
    }

    private static IllegalArgumentException unsupported(String domain, String typeName) {
        return new IllegalArgumentException(
                "Unsupported " + domain + " type: " + typeName);
    }

    /**
     * Extracts a Comparable value from a Paimon InternalRow at the given index,
     * using the DataType to select the correct getter. Returns null for unsupported types.
     */
    public static Comparable<?> extractComparable(
            org.apache.paimon.data.InternalRow row, int index, DataType type) {
        if (row.isNullAt(index)) {
            return null;
        }
        return switch (type.getTypeRoot()) {
            case INTEGER -> row.getInt(index);
            case BIGINT -> row.getLong(index);
            case SMALLINT -> row.getShort(index);
            case TINYINT -> row.getByte(index);
            case FLOAT -> row.getFloat(index);
            case DOUBLE -> row.getDouble(index);
            case BOOLEAN -> row.getBoolean(index);
            case VARCHAR, CHAR -> row.getString(index).toString();
            default -> null;
        };
    }

}
