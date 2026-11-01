package org.pangrova.indexing.core.serde;

import org.pangrova.spi.indexing.descriptor.IndexType;

import java.util.EnumMap;
import java.util.Map;

/**
 * Registry mapping {@link IndexType} to its {@link TypeSerializer}.
 * Immutable after construction.
 */
public final class TypeSerializerRegistry {

    private final Map<IndexType, TypeSerializer<?>> serializers;

    private TypeSerializerRegistry(Map<IndexType, TypeSerializer<?>> serializers) {
        this.serializers = Map.copyOf(serializers);
    }

    /**
     * Returns the serializer for the given index type.
     *
     * @throws IllegalArgumentException if no serializer is registered
     */
    public TypeSerializer<?> get(IndexType type) {
        TypeSerializer<?> s = serializers.get(type);
        if (s == null) {
            throw new IllegalArgumentException("No TypeSerializer registered for " + type);
        }
        return s;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final EnumMap<IndexType, TypeSerializer<?>> map = new EnumMap<>(IndexType.class);

        public Builder register(IndexType type, TypeSerializer<?> serializer) {
            TypeSerializer<?> existing = map.putIfAbsent(type, serializer);
            if (existing != null) {
                throw new IllegalStateException("Duplicate TypeSerializer for " + type);
            }
            return this;
        }

        public TypeSerializerRegistry build() {
            return new TypeSerializerRegistry(map);
        }
    }
}
