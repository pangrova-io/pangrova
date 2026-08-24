package org.pangrova.spi.connector.data.type;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.annotation.Experimental;

/**
 * Key-value mapping type.
 *
 * @param keyType   the type of map keys
 * @param valueType the type of map values
 */
@Experimental
public record MapType(Type keyType, Type valueType) implements Type {

    public MapType {
        Objects.requireNonNull(keyType, "keyType must not be null");
        Objects.requireNonNull(valueType, "valueType must not be null");
    }

    @Override
    public String getDisplayName() {
        return "MAP(" + keyType.getDisplayName() + "," + valueType.getDisplayName() + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("MAP", List.of(
                TypeSignature.TypeParameter.ofType(keyType.getTypeSignature()),
                TypeSignature.TypeParameter.ofType(valueType.getTypeSignature())));
    }
}
