package org.pangrova.connector.spi.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
