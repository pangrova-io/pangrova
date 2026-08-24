package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * 32-bit IEEE 754 floating point type.
 */
@Experimental
public record RealType() implements Type {

    public static final RealType INSTANCE = new RealType();

    @Override
    public String getDisplayName() {
        return "REAL";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("REAL");
    }
}
