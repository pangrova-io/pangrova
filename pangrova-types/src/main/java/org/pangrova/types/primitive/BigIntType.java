package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * 64-bit signed integer type.
 */
@Experimental
public record BigIntType() implements Type {

    public static final BigIntType INSTANCE = new BigIntType();

    @Override
    public String getDisplayName() {
        return "BIGINT";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("BIGINT");
    }
}
