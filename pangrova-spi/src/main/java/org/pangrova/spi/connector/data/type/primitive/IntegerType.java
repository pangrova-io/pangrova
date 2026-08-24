package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * 32-bit signed integer type.
 */
@Experimental
public record IntegerType() implements Type {

    public static final IntegerType INSTANCE = new IntegerType();

    @Override
    public String getDisplayName() {
        return "INTEGER";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("INTEGER");
    }
}
