package org.pangrova.spi.connector.data.type.primitive;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

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
