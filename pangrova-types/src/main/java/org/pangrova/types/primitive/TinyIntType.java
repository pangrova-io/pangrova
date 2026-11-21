package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * 8-bit signed integer type.
 */
@Experimental
public record TinyIntType() implements Type {

    public static final TinyIntType INSTANCE = new TinyIntType();

    @Override
    public String getDisplayName() {
        return "TINYINT";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("TINYINT");
    }
}
