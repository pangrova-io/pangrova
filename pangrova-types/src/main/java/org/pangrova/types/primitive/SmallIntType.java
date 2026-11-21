package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * 16-bit signed integer type.
 */
@Experimental
public record SmallIntType() implements Type {

    public static final SmallIntType INSTANCE = new SmallIntType();

    @Override
    public String getDisplayName() {
        return "SMALLINT";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("SMALLINT");
    }
}
