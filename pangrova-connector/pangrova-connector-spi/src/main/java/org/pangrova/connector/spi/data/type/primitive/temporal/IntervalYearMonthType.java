package org.pangrova.connector.spi.data.type.primitive.temporal;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

/**
 * Year-to-month interval type for date arithmetic.
 */
@Experimental
public final class IntervalYearMonthType implements Type {

    public static final IntervalYearMonthType INSTANCE = new IntervalYearMonthType();

    private IntervalYearMonthType() {}

    @Override
    public String getDisplayName() {
        return "INTERVAL YEAR TO MONTH";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("INTERVAL_YEAR_MONTH", List.of());
    }
}
