package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

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
