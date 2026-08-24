package org.pangrova.planner.substrait;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps Calcite RelDataType to Substrait Type, respecting nullability.
 */
public class TypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypeMapper.class);

    /**
     * Map a Calcite RelDataType to the corresponding Substrait Type.
     *
     * @throws SubstraitConversionException for unsupported types
     */
    public Type map(RelDataType calciteType) {
        TypeCreator creator = TypeCreator.of(calciteType.isNullable());
        return mapByTypeName(creator, calciteType);
    }

    /**
     * Converts a Calcite RelDataType to the parameterized Pangrova
     * {@link org.pangrova.spi.connector.data.Type}. For DECIMAL and VARCHAR,
     * preserves precision/scale/length. For simple types, bridges via
     * {@link PangrovaTypeMapping}.
     *
     * @param calciteType the Calcite type to convert
     * @return the corresponding parameterized Pangrova Type
     * @throws SubstraitConversionException for unsupported types
     */
    public org.pangrova.spi.connector.data.type.Type toPangrovaParameterizedType(
            RelDataType calciteType) {
        SqlTypeName sqlType = calciteType.getSqlTypeName();
        return mapToParameterized(sqlType, calciteType);
    }

    private org.pangrova.spi.connector.data.type.Type mapToParameterized(
            SqlTypeName sqlType, RelDataType calciteType) {
        return switch (sqlType) {
            case DECIMAL -> {
                int p = calciteType.getPrecision();
                int s = calciteType.getScale();
                if (p <= 0 || p == Integer.MIN_VALUE) {
                    p = 38;
                    s = Math.max(s, 0);
                }
                if (s < 0 || s > p) {
                    s = Math.min(Math.max(s, 0), p);
                }
                yield new DecimalType(p, s);
            }
            case VARCHAR, CHAR -> resolveVarchar(calciteType);
            case NULL -> VarcharType.UNBOUNDED;
            default -> bridgeViaPangrovaType(sqlType);
        };
    }

    private org.pangrova.spi.connector.data.type.Type resolveVarchar(RelDataType calciteType) {
        int precision = calciteType.getPrecision();
        if (precision <= 0 || precision == Integer.MAX_VALUE) {
            return VarcharType.UNBOUNDED;
        }
        return new VarcharType(precision);
    }

    private org.pangrova.spi.connector.data.type.Type bridgeViaPangrovaType(SqlTypeName sqlType) {
        String name = sqlType.getName();
        try {
            PangrovaType pangrovaType = PangrovaType.fromSqlTypeName(name);
            return PangrovaTypeMapping.toType(pangrovaType);
        } catch (IllegalArgumentException e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Unsupported Calcite type for parameterized mapping: " + name);
        }
    }

    private Type mapByTypeName(TypeCreator creator, RelDataType calciteType) {
        Type primitive = mapPrimitiveType(creator, calciteType.getSqlTypeName());
        if (primitive != null) {
            return primitive;
        }
        return mapComplexType(creator, calciteType);
    }

    private Type mapPrimitiveType(TypeCreator creator, SqlTypeName typeName) {
        return switch (typeName) {
            case TINYINT -> creator.I8;
            case SMALLINT -> creator.I16;
            case INTEGER -> creator.I32;
            case BIGINT -> creator.I64;
            case FLOAT, REAL -> creator.FP32;
            case DOUBLE -> creator.FP64;
            case VARCHAR, CHAR -> creator.STRING;
            case BOOLEAN -> creator.BOOLEAN;
            case DATE -> creator.DATE;
            case TIME -> creator.TIME;
            case TIMESTAMP -> creator.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> creator.TIMESTAMP_TZ;
            case BINARY, VARBINARY -> creator.BINARY;
            // NULL type: Calcite represents untyped NULL literals as SqlTypeName.NULL.
            // Substrait has no "nothing" type; we map to nullable BOOLEAN as the smallest
            // representation. This allows SELECT NULL and similar expressions to produce
            // valid Substrait plans without requiring explicit CAST/COALESCE.
            case NULL -> TypeCreator.of(true).BOOLEAN;
            default -> null;
        };
    }

    private Type mapComplexType(TypeCreator creator, RelDataType calciteType) {
        return switch (calciteType.getSqlTypeName()) {
            case DECIMAL -> mapDecimalType(creator, calciteType);
            case ARRAY -> mapArrayType(creator, calciteType);
            case MAP -> mapMapType(creator, calciteType);
            // ROW = Calcite struct type with named fields (declared via
            // CREATE TABLE (info STRUCT(...)) or anonymous ROW('a', 1)). Both
            // map to a Substrait struct over their declared field types. Note:
            // SqlTypeName.STRUCTURED is reserved for user-defined types and
            // does not carry a fieldList — STRUCT columns must be exposed as
            // ROW from the catalog reader (see MetadataResolvedTable).
            case ROW -> mapRowType(creator, calciteType);
            default -> mapIntervalOrThrow(creator, calciteType);
        };
    }

    /**
     * Maps DECIMAL with defensive precision/scale handling.
     * Calcite constant folding can produce Integer.MIN_VALUE precision for folded literals;
     * we normalize to default 38/10 in that case.
     */
    private Type mapDecimalType(TypeCreator creator, RelDataType calciteType) {
        int precision = calciteType.getPrecision();
        int scale = calciteType.getScale();
        if (precision <= 0 || precision == Integer.MIN_VALUE) {
            LOG.warn("event=decimal_abnormal_precision original_precision={} original_scale={} "
                            + "action=normalize_to_default",
                    precision, scale);
            precision = 38;
            scale = Math.max(scale, 0);
            if (scale > precision) {
                scale = 10;
            }
        }
        if (scale < 0 || scale > precision) {
            scale = Math.min(Math.max(scale, 0), precision);
        }
        return creator.decimal(precision, scale);
    }

    private Type mapIntervalOrThrow(TypeCreator creator, RelDataType calciteType) {
        return switch (calciteType.getSqlTypeName()) {
            case INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> creator.INTERVAL_YEAR;
            case INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR,
                    INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND,
                    INTERVAL_SECOND -> creator.INTERVAL_DAY;
            default -> throw unsupportedType(calciteType);
        };
    }

    private Type mapArrayType(TypeCreator creator, RelDataType calciteType) {
        Type elementType = mapByTypeName(creator, calciteType.getComponentType());
        return creator.list(elementType);
    }

    private Type mapMapType(TypeCreator creator, RelDataType calciteType) {
        Type keyType = mapByTypeName(creator, calciteType.getKeyType());
        Type valueType = mapByTypeName(creator, calciteType.getValueType());
        return creator.map(keyType, valueType);
    }

    private Type mapRowType(TypeCreator creator, RelDataType calciteType) {
        java.util.List<Type> fieldTypes = calciteType.getFieldList().stream()
                .map(f -> mapByTypeName(creator, f.getType()))
                .toList();
        return creator.struct(fieldTypes);
    }

    private static SubstraitConversionException unsupportedType(RelDataType calciteType) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                "Unsupported Calcite type: " + calciteType.getSqlTypeName());
    }
}
