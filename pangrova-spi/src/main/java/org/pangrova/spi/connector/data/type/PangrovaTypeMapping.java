package org.pangrova.spi.connector.data.type;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.primitive.BooleanType;
import org.pangrova.spi.connector.data.type.primitive.CharType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.DoubleType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;
import org.pangrova.spi.connector.data.type.primitive.RealType;
import org.pangrova.spi.connector.data.type.primitive.SmallIntType;
import org.pangrova.spi.connector.data.type.primitive.TinyIntType;
import org.pangrova.spi.connector.data.type.primitive.VarbinaryType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.DateType;
import org.pangrova.spi.connector.data.type.primitive.temporal.IntervalDayTimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.IntervalYearMonthType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimeType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;

/**
 * Static utility bridging the legacy {@link PangrovaType} enum to the new
 * parameterized {@link Type} interface, enabling gradual migration.
 *
 * <p>For parameterized types (DECIMAL, VARCHAR, ARRAY, MAP, STRUCT),
 * the bridge uses sensible defaults since PangrovaType carries no parameters.</p>
 */
@Experimental
public final class PangrovaTypeMapping {

    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 18;

    private PangrovaTypeMapping() {}

    /**
     * Converts an {@link PangrovaType} enum value to a {@link Type} instance.
     * Parameterized types use default parameters (e.g. DECIMAL → DECIMAL(38,18)).
     *
     * @param pangrovaType the legacy enum value
     * @return the corresponding Type instance
     * @throws IllegalArgumentException if the enum value has no mapping
     */
    public static Type toType(PangrovaType pangrovaType) {
        return switch (pangrovaType) {
            case BOOLEAN -> BooleanType.INSTANCE;
            case TINYINT -> TinyIntType.INSTANCE;
            case SMALLINT -> SmallIntType.INSTANCE;
            case INTEGER -> IntegerType.INSTANCE;
            case BIGINT -> BigIntType.INSTANCE;
            case FLOAT -> RealType.INSTANCE;
            case DOUBLE -> DoubleType.INSTANCE;
            case DECIMAL -> new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            case DATE -> DateType.INSTANCE;
            case TIME -> TimeType.INSTANCE;
            case TIMESTAMP -> TimestampType.WITHOUT_TIMEZONE;
            case TIMESTAMP_TZ -> TimestampType.WITH_TIMEZONE;
            case VARCHAR, UUID -> VarcharType.UNBOUNDED;
            case VARBINARY, FIXED -> VarbinaryType.INSTANCE;
            case STRUCT -> new RowType(java.util.List.of(
                    new RowField("_placeholder", VarcharType.UNBOUNDED)));
            case ARRAY -> new ArrayType(VarcharType.UNBOUNDED);
            case MAP -> new MapType(VarcharType.UNBOUNDED, VarcharType.UNBOUNDED);
            case INTERVAL_YEAR_MONTH -> IntervalYearMonthType.INSTANCE;
            case INTERVAL_DAY_TIME -> IntervalDayTimeType.INSTANCE;
            case VECTOR -> null; // dimension unknown without DDL context
            case GEOMETRY, GEOGRAPHY -> VarbinaryType.INSTANCE;
            case VARIANT -> VarcharType.UNBOUNDED;
            case TIMESTAMP_NS -> TimestampType.WITHOUT_TIMEZONE;
            case UNKNOWN -> VarcharType.UNBOUNDED;
        };
    }

    /**
     * Converts a {@link Type} instance back to the closest {@link PangrovaType} enum.
     * Parameter information is lost in the conversion.
     *
     * @param type the parameterized type
     * @return the corresponding PangrovaType enum value
     * @throws IllegalArgumentException if the type has no enum mapping
     */
    public static PangrovaType toPangrovaType(Type type) {
        PangrovaType mappedType = numericType(type);
        if (mappedType != null) {
            return mappedType;
        }
        if (type instanceof TimestampType ts) {
            return ts.withTimezone() ? PangrovaType.TIMESTAMP_TZ : PangrovaType.TIMESTAMP;
        }
        mappedType = structuralType(type);
        if (mappedType != null) {
            return mappedType;
        }
        throw new IllegalArgumentException(
                "No PangrovaType mapping for: " + type.getDisplayName());
    }

    private static PangrovaType numericType(Type type) {
        if (type instanceof BooleanType) {
            return PangrovaType.BOOLEAN;
        }
        if (type instanceof TinyIntType) {
            return PangrovaType.TINYINT;
        }
        if (type instanceof SmallIntType) {
            return PangrovaType.SMALLINT;
        }
        if (type instanceof IntegerType) {
            return PangrovaType.INTEGER;
        }
        if (type instanceof BigIntType) {
            return PangrovaType.BIGINT;
        }
        return floatingPointType(type);
    }

    private static PangrovaType floatingPointType(Type type) {
        if (type instanceof RealType) {
            return PangrovaType.FLOAT;
        }
        if (type instanceof DoubleType) {
            return PangrovaType.DOUBLE;
        }
        if (type instanceof DecimalType) {
            return PangrovaType.DECIMAL;
        }
        if (type instanceof DateType) {
            return PangrovaType.DATE;
        }
        if (type instanceof TimeType) {
            return PangrovaType.TIME;
        }
        return null;
    }

    private static PangrovaType structuralType(Type type) {
        if (type instanceof VarcharType || type instanceof CharType) {
            return PangrovaType.VARCHAR;
        }
        if (type instanceof VarbinaryType) {
            return PangrovaType.VARBINARY;
        }
        if (type instanceof ArrayType) {
            return PangrovaType.ARRAY;
        }
        if (type instanceof MapType) {
            return PangrovaType.MAP;
        }
        if (type instanceof RowType) {
            return PangrovaType.STRUCT;
        }
        return specializedType(type);
    }

    private static PangrovaType specializedType(Type type) {
        if (type instanceof IntervalYearMonthType) {
            return PangrovaType.INTERVAL_YEAR_MONTH;
        }
        if (type instanceof IntervalDayTimeType) {
            return PangrovaType.INTERVAL_DAY_TIME;
        }
        if (type instanceof VectorType) {
            return PangrovaType.VECTOR;
        }
        return null;
    }

    /**
     * Parses a SQL type name string into a parameterized {@link Type} instance.
     * Handles {@code VECTOR(dimension)} and {@code VECTOR(dimension, elementType)}
     * syntax, constructing the appropriate {@link VectorType}.
     *
     * <p>For non-VECTOR types, delegates to {@link #toType(PangrovaType)} via
     * {@link PangrovaType#fromSqlTypeName(String)}.</p>
     *
     * @param sqlTypeName the SQL type name (e.g. "VECTOR(128)", "VECTOR(384, DOUBLE)")
     * @return the corresponding Type instance, or null for VECTOR without parameters
     * @throws IllegalArgumentException if the type name is not recognized or parameters are invalid
     */
    public static Type fromSqlTypeName(String sqlTypeName) {
        String upper = sqlTypeName.strip().toUpperCase();
        if (upper.startsWith("VECTOR")) {
            return parseVectorType(sqlTypeName.strip());
        }
        if (upper.equals("FILE")) {
            return new RowType(java.util.List.of(
                    new RowField("uri", VarcharType.UNBOUNDED),
                    new RowField("mime_type", VarcharType.UNBOUNDED),
                    new RowField("size", BigIntType.INSTANCE),
                    new RowField("checksum", VarcharType.UNBOUNDED),
                    new RowField("created_at", TimestampType.WITHOUT_TIMEZONE)
            ));
        }
        return toType(PangrovaType.fromSqlTypeName(sqlTypeName));
    }

    private static VectorType parseVectorType(String sqlTypeName) {
        String upper = sqlTypeName.toUpperCase();
        int openParen = upper.indexOf('(');
        int closeParen = upper.lastIndexOf(')');
        if (openParen < 0 || closeParen < 0 || closeParen <= openParen) {
            throw new IllegalArgumentException(
                    "VECTOR type requires dimension parameter: VECTOR(dimension[, FLOAT|DOUBLE]), got: " + sqlTypeName);
        }
        String inner = sqlTypeName.substring(openParen + 1, closeParen).strip();
        String[] parts = inner.split(",", 2);
        int dimension = parseDimension(parts[0].strip());
        VectorType.ElementType elementType = parts.length == 2
                ? parseElementType(parts[1].strip()) : VectorType.ElementType.FLOAT;
        return new VectorType(dimension, elementType);
    }

    private static int parseDimension(String raw) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid VECTOR dimension: " + raw, e);
        }
    }

    private static VectorType.ElementType parseElementType(String raw) {
        return switch (raw.toUpperCase()) {
            case "FLOAT" -> VectorType.ElementType.FLOAT;
            case "DOUBLE" -> VectorType.ElementType.DOUBLE;
            default -> throw new IllegalArgumentException(
                    "Unsupported VECTOR element type: " + raw + ". Expected FLOAT or DOUBLE");
        };
    }
}
