package org.pangrova.spi.connector.data.type;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A storage column whose only legal value is NULL, as in Iceberg v3 unknown.
 * This is distinct from an unresolved {@link PangrovaType#UNKNOWN} declaration.
 */
@Experimental
public record NullOnlyType() implements Type {

    /** Shared immutable null-only declaration. */
    public static final NullOnlyType INSTANCE = new NullOnlyType();

    @Override
    public String getDisplayName() {
        return "NULL_ONLY";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("NULL_ONLY");
    }
}
