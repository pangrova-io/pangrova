package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/** A 128-bit universally unique identifier, distinct from strings and binary values. */
@Experimental
public record UuidType() implements Type {
    public static final UuidType INSTANCE = new UuidType();

    @Override
    public String getDisplayName() {
        return "UUID";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("UUID");
    }
}
