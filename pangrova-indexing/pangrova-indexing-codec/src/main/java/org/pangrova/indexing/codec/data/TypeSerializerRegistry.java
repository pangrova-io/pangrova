package org.pangrova.indexing.codec.data;

import org.pangrova.indexing.api.codec.TypeSerializer;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;

import org.pangrova.indexing.api.enums.IndexType;

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

    /** Binds storage codecs to the exact strategy instances selected for this process. */
    public static TypeSerializerRegistry fromStrategies(Iterable<IndexStrategyProvider> strategies) {
        var builder = builder();
        for (IndexStrategyProvider strategy : strategies) {
            builder.register(strategy.supportedType(), strategy.payloadSerializer());
        }
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final EnumMap<IndexType, TypeSerializer<?>> map = new EnumMap<>(IndexType.class);

        /**
         * Registers one serializer without replacing an existing owner of the type.
         *
         * @throws IllegalStateException if the type is already registered
         */
        public Builder register(IndexType type, TypeSerializer<?> serializer) {
            java.util.Objects.requireNonNull(serializer, "missing payload serializer for " + type);
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
