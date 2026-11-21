package org.pangrova.types.primitive.temporal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

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
