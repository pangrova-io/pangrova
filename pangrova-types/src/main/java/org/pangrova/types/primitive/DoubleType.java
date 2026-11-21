package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

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
