package org.pangrova.indexing.builtin.codec;

import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.builtin.bitmap.serializer.BitmapSerializer;
import org.pangrova.indexing.builtin.bloom.serializer.BloomFilterSerializer;
import org.pangrova.indexing.builtin.btree.serializer.BTreeSerializer;
import org.pangrova.indexing.builtin.gin.serializer.GinSerializer;
import org.pangrova.indexing.builtin.hash.serializer.HashSerializer;
import org.pangrova.indexing.builtin.ngram.serializer.NGramBloomSerializer;
import org.pangrova.indexing.builtin.prefix.serializer.PrefixSerializer;
import org.pangrova.indexing.builtin.rtree.serializer.RTreeSerializer;
import org.pangrova.indexing.builtin.sparse.serializer.SparseSerializer;

/** Assembles the built-in serializers without coupling the codec to their implementations. */
public final class BuiltinIndexSerializers {
    private BuiltinIndexSerializers() {}

    /**
     * Creates the default registry for JVM file-pruning indexes.
     * Durable ANN artifacts are produced and consumed only by Native workers.
     *
     * @return a registry containing the built-in JVM serializers
     */
    public static TypeSerializerRegistry createRegistry() {
        return TypeSerializerRegistry.builder()
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
}
