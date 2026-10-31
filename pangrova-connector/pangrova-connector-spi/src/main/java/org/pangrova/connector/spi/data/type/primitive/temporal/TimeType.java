package org.pangrova.connector.spi.data.type.primitive.temporal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

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
