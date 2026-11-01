package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * 64-bit IEEE 754 floating point type.
 */
@Experimental
public record DoubleType() implements Type {

    public static final DoubleType INSTANCE = new DoubleType();

    @Override
    public String getDisplayName() {
        return "DOUBLE";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("DOUBLE");
    }
}
