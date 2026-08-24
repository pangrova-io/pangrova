package org.pangrova.spi.connector.data.type.primitive.temporal;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;

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
