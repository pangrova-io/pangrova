package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
