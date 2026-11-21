package org.pangrova.types.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * Variable-length binary data type.
 */
@Experimental
public record VarbinaryType() implements Type {

    public static final VarbinaryType INSTANCE = new VarbinaryType();

    @Override
    public String getDisplayName() {
        return "VARBINARY";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("VARBINARY");
    }
}
