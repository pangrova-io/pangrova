package org.pangrova.indexing.serde;

import org.pangrova.spi.index.IndexType;

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
     * Creates the default registry for JVM file-pruning indexes.
     * Durable ANN artifacts are produced and consumed only by Native workers.
     */
    public static TypeSerializerRegistry createDefault() {
        return builder()
                .register(IndexType.BTREE, new BTreeSerializer())
                .register(IndexType.BLOOM_FILTER, new BloomFilterSerializer())
                .register(IndexType.PREFIX, new PrefixSerializer())
                .register(IndexType.NGRAM_BLOOM_FILTER, new NGramBloomSerializer())
                .register(IndexType.BITMAP, new BitmapSerializer())
                .register(IndexType.HASH, new HashSerializer())
                .register(IndexType.SPARSE, new SparseSerializer())
                .register(IndexType.R_TREE, new RTreeSerializer())
                .register(IndexType.GIN, new GinSerializer())
                .build();
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
            TypeSerializer<?> existing = map.put(type, serializer);
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
