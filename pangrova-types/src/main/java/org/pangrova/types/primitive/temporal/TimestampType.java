package org.pangrova.types.primitive.temporal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * Timestamp type with microsecond precision.
 *
 * @param withTimezone whether this timestamp includes timezone information
 */
@Experimental
public record TimestampType(boolean withTimezone) implements Type {

    public static final TimestampType WITHOUT_TIMEZONE = new TimestampType(false);
    public static final TimestampType WITH_TIMEZONE = new TimestampType(true);

    @Override
    public String getDisplayName() {
        return withTimezone ? "TIMESTAMP WITH TIME ZONE" : "TIMESTAMP";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature(withTimezone ? "TIMESTAMP_TZ" : "TIMESTAMP");
    }
}
