package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.mapping.PangrovaTypeConverter;
import org.pangrova.types.primitive.VarcharType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.planner.type.DecimalTypeMapping;

/**
 * Maps Calcite RelDataType to Substrait Type, respecting nullability.
 */
public class TypeMapper {

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
     * {@link org.pangrova.types.Type}. For DECIMAL and VARCHAR,
     * preserves precision/scale/length. For simple types, bridges via
     * {@link PangrovaTypeConverter}.
     *
     * @param calciteType the Calcite type to convert
     * @return the corresponding parameterized Pangrova Type
     * @throws SubstraitConversionException for unsupported types
     */
    public org.pangrova.types.Type toPangrovaParameterizedType(
            RelDataType calciteType) {
        SqlTypeName sqlType = calciteType.getSqlTypeName();
        return mapToParameterized(sqlType, calciteType);
    }

    private org.pangrova.types.Type mapToParameterized(
            SqlTypeName sqlType, RelDataType calciteType) {
        return switch (sqlType) {
            case DECIMAL -> DecimalTypeMapping.toPangrova(calciteType);
            case VARCHAR, CHAR -> resolveVarchar(calciteType);
            case NULL -> VarcharType.UNBOUNDED;
            default -> bridgeViaPangrovaType(sqlType);
        };
    }

    private org.pangrova.types.Type resolveVarchar(RelDataType calciteType) {
        int precision = calciteType.getPrecision();
        if (precision <= 0 || precision == Integer.MAX_VALUE) {
            return VarcharType.UNBOUNDED;
        }
        return new VarcharType(precision);
    }

    private org.pangrova.types.Type bridgeViaPangrovaType(SqlTypeName sqlType) {
        String name = sqlType.getName();
        try {
            PangrovaType pangrovaType = PangrovaType.fromSqlTypeName(name);
            return PangrovaTypeConverter.toType(pangrovaType);
        } catch (IllegalArgumentException e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Unsupported Calcite type for parameterized mapping: " + name);
        }
    }

    private Type mapByTypeName(TypeCreator creator, RelDataType calciteType) {
        Type primitive = mapPrimitiveType(creator, calciteType);
        if (primitive != null) {
            return primitive;
        }
        return mapComplexType(creator, calciteType);
    }

    private Type mapPrimitiveType(TypeCreator creator, RelDataType calciteType) {
        return switch (calciteType.getSqlTypeName()) {
            case TINYINT -> creator.I8;
            case SMALLINT -> creator.I16;
            case INTEGER -> creator.I32;
            case BIGINT -> creator.I64;
            case FLOAT, REAL -> creator.FP32;
            case DOUBLE -> creator.FP64;
            case VARCHAR, CHAR -> creator.STRING;
            case BOOLEAN -> creator.BOOLEAN;
            case DATE -> creator.DATE;
            case TIME -> creator.precisionTime(temporalPrecision(calciteType));
            case TIMESTAMP -> creator.precisionTimestamp(temporalPrecision(calciteType));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                    creator.precisionTimestampTZ(temporalPrecision(calciteType));
            case BINARY, VARBINARY -> creator.BINARY;
            case UUID -> creator.UUID;
            // NULL type: Calcite represents untyped NULL literals as SqlTypeName.NULL.
            // Substrait has no "nothing" type; we map to nullable BOOLEAN as the smallest
            // representation. This allows SELECT NULL and similar expressions to produce
            // valid Substrait plans without requiring explicit CAST/COALESCE.
            case NULL -> TypeCreator.of(true).BOOLEAN;
            default -> null;
        };
    }

    private static int temporalPrecision(RelDataType type) {
        int precision = type.getPrecision();
        return precision < 0 || precision == Integer.MAX_VALUE
                ? 6
                : Math.min(precision, 9);
    }

    private Type mapComplexType(TypeCreator creator, RelDataType calciteType) {
        return switch (calciteType.getSqlTypeName()) {
            case DECIMAL -> DecimalTypeMapping.toSubstrait(calciteType);
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

    private Type mapIntervalOrThrow(TypeCreator creator, RelDataType calciteType) {
        return switch (calciteType.getSqlTypeName()) {
            case INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> creator.INTERVAL_YEAR;
            case INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR,
                    INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND,
                    INTERVAL_SECOND -> creator.intervalDay(6);
            default -> throw unsupportedType(calciteType);
        };
    }

    private Type mapArrayType(TypeCreator creator, RelDataType calciteType) {
        var component = calciteType.getComponentType();
        if (component == null) throw unsupportedType(calciteType);
        Type elementType = map(component);
        return creator.list(elementType);
    }

    private Type mapMapType(TypeCreator creator, RelDataType calciteType) {
        var key = calciteType.getKeyType();
        var value = calciteType.getValueType();
        if (key == null || value == null) throw unsupportedType(calciteType);
        return creator.map(map(key), map(value));
    }

    private Type mapRowType(TypeCreator creator, RelDataType calciteType) {
        java.util.List<Type> fieldTypes = calciteType.getFieldList().stream()
                .map(f -> map(f.getType()))
                .toList();
        return creator.struct(fieldTypes);
    }

    private static SubstraitConversionException unsupportedType(RelDataType calciteType) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                "Unsupported Calcite type: " + calciteType.getSqlTypeName());
    }
}
