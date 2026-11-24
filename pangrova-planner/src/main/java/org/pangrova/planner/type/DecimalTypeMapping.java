package org.pangrova.planner.type;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.type.RelDataType;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.planner.exception.SubstraitConversionException;
import org.pangrova.types.primitive.DecimalType;

/** Preserves Calcite decimal precision and scale in planner and wire types. */
public final class DecimalTypeMapping {
    private DecimalTypeMapping() {}

    /** Maps a validated decimal to the parameterized Pangrova type. */
    public static DecimalType toPangrova(RelDataType type) {
        validate(type);
        return new DecimalType(type.getPrecision(), type.getScale());
    }

    /** Maps a validated decimal to the Substrait type with input nullability. */
    public static Type toSubstrait(RelDataType type) {
        validate(type);
        return TypeCreator.of(type.isNullable()).decimal(type.getPrecision(), type.getScale());
    }

    private static void validate(RelDataType type) {
        try {
            PlannerTypeSystem.INSTANCE.validateDecimal(type.getPrecision(), type.getScale());
        } catch (IllegalArgumentException failure) {
            throw new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Invalid DECIMAL type: " + failure.getMessage(), failure);
        }
    }
}
