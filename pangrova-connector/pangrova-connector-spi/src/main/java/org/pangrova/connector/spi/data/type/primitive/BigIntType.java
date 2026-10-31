package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
