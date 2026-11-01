package org.pangrova.spi.connector.data.type.primitive.temporal;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * Day-to-second interval type for time arithmetic.
 */
@Experimental
public final class IntervalDayTimeType implements Type {

    public static final IntervalDayTimeType INSTANCE = new IntervalDayTimeType();

    private IntervalDayTimeType() {}

    @Override
    public String getDisplayName() {
        return "INTERVAL DAY TO SECOND";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("INTERVAL_DAY_TIME", List.of());
    }
}
