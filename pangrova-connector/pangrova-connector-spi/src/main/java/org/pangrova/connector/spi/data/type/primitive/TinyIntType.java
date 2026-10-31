package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

/**
 * 8-bit signed integer type.
 */
@Experimental
public record TinyIntType() implements Type {

    public static final TinyIntType INSTANCE = new TinyIntType();

    @Override
    public String getDisplayName() {
        return "TINYINT";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("TINYINT");
    }
}
