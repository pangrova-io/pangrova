package org.pangrova.connector.spi.data.type.primitive.temporal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.type.Type;
import org.pangrova.connector.spi.data.type.TypeSignature;

/**
 * Calendar date type (year, month, day).
 */
@Experimental
public record DateType() implements Type {

    public static final DateType INSTANCE = new DateType();

    @Override
    public String getDisplayName() {
        return "DATE";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("DATE");
    }
}
