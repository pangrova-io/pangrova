package org.pangrova.types.primitive;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * Variable-length character string type with optional maximum length.
 *
 * @param length maximum length in characters; {@link Integer#MAX_VALUE} for unbounded
 */
@Experimental
public record VarcharType(int length) implements Type {

    /** Unbounded VARCHAR (no length limit). */
    public static final VarcharType UNBOUNDED = new VarcharType(Integer.MAX_VALUE);

    public VarcharType {
        if (length < 1) {
            throw new IllegalArgumentException("Length must be >= 1, got: " + length);
        }
    }

    @Override
    public String getDisplayName() {
        if (length == Integer.MAX_VALUE) {
            return "VARCHAR";
        }
        return "VARCHAR(" + length + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("VARCHAR", List.of(
                TypeSignature.TypeParameter.ofLong(length)));
    }
}
