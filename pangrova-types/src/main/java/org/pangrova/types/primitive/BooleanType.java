package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * Boolean type (true/false).
 */
@Experimental
public record BooleanType() implements Type {

    public static final BooleanType INSTANCE = new BooleanType();

    @Override
    public String getDisplayName() {
        return "BOOLEAN";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("BOOLEAN");
    }
}
