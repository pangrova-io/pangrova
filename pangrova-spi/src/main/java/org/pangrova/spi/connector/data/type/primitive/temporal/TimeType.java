package org.pangrova.spi.connector.data.type.primitive.temporal;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

/**
 * Time of day type without timezone.
 */
@Experimental
public record TimeType() implements Type {

    public static final TimeType INSTANCE = new TimeType();

    @Override
    public String getDisplayName() {
        return "TIME";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("TIME");
    }
}
