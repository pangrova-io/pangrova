package org.pangrova.planner.type;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/** SQL precision and decimal arithmetic supported by the Native Worker. */
public final class PlannerTypeSystem extends RelDataTypeSystemImpl {

    /** Shared immutable type system used by production planning. */
    public static final PlannerTypeSystem INSTANCE = new PlannerTypeSystem();

    private PlannerTypeSystem() {}

    /** Rejects malformed declarations and generated types before any value conversion. */
    public void validateDecimal(int precision, int scale) {
        int maximum = getMaxPrecision(SqlTypeName.DECIMAL);
        if (precision < 1 || precision > maximum) {
            throw new IllegalArgumentException(
                    "DECIMAL precision must be between 1 and " + maximum + ": " + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "DECIMAL scale must be between 0 and precision " + precision + ": " + scale);
        }
    }

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        return switch (typeName) {
            case DECIMAL -> 38;
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ -> 9;
            case TIME, TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ -> 6;
            default -> super.getMaxPrecision(typeName);
        };
    }

    @Override
    public int getMaxScale(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? getMaxPrecision(typeName) : super.getMaxScale(typeName);
    }

    @Override
    public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory factory,
            RelDataType left, RelDataType right) {
        return DecimalArithmeticTypeDerivation.multiply(factory, left, right);
    }

    @Override
    public RelDataType deriveDecimalDivideType(RelDataTypeFactory factory,
            RelDataType left, RelDataType right) {
        return DecimalArithmeticTypeDerivation.divide(factory, left, right);
    }

    @Override
    public RelDataType deriveDecimalModType(RelDataTypeFactory factory,
            RelDataType left, RelDataType right) {
        return DecimalArithmeticTypeDerivation.modulus(factory, left, right);
    }

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        return switch (typeName) {
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ,
                    TIME, TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ -> 6;
            default -> super.getDefaultPrecision(typeName);
        };
    }
}
