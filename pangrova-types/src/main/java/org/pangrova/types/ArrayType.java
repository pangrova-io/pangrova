package org.pangrova.types;

import java.util.List;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Ordered collection type with a single element type.
 *
 * @param elementType the type of elements in the array
 */
@Experimental
public record ArrayType(Type elementType) implements Type {

    public ArrayType {
        Objects.requireNonNull(elementType, "elementType must not be null");
    }

    @Override
    public String getDisplayName() {
        return "ARRAY(" + elementType.getDisplayName() + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("ARRAY", List.of(
                TypeSignature.TypeParameter.ofType(elementType.getTypeSignature())));
    }
}
