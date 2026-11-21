package org.pangrova.types.primitive.temporal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

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
