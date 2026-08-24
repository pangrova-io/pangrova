package org.pangrova.spi.connector.data.type.primitive;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * Arbitrary-precision decimal type with configurable precision and scale.
 *
 * @param precision total number of digits (1-38)
 * @param scale     number of digits after the decimal point (0-precision)
 */
@Experimental
public record DecimalType(int precision, int scale) implements Type {

    private static final int MAX_PRECISION = 38;

    public DecimalType {
        if (precision < 1 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    "Precision must be between 1 and 38, got: " + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "Scale must be between 0 and precision (" + precision + "), got: " + scale);
        }
    }

    @Override
    public String getDisplayName() {
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("DECIMAL", List.of(
                TypeSignature.TypeParameter.ofLong(precision),
                TypeSignature.TypeParameter.ofLong(scale)));
    }
}
