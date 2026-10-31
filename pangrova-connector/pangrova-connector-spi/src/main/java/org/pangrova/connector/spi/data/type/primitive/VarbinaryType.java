package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
