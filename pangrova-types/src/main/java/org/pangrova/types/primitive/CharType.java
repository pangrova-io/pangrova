package org.pangrova.types.primitive;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;

/**
 * Fixed-length character string type.
 *
 * @param length fixed length in characters (1-255)
 */
@Experimental
public record CharType(int length) implements Type {

    private static final int MAX_LENGTH = 255;

    public CharType {
        if (length < 1 || length > MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "Length must be between 1 and 255, got: " + length);
        }
    }

    @Override
    public String getDisplayName() {
        return "CHAR(" + length + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("CHAR", List.of(
                TypeSignature.TypeParameter.ofLong(length)));
    }
}
