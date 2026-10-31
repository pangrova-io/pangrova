package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
