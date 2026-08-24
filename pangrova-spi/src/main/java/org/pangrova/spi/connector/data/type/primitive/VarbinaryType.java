package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

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
