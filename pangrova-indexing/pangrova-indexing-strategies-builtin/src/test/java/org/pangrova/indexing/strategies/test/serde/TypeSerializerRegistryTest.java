package org.pangrova.indexing.strategies.test.serde;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.codec.TypeSerializer;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.strategies.bloom.serializer.BloomFilterSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeSerializerRegistryTest {
    @Test
    void rejectedDuplicateDoesNotReplaceRegisteredSerializer() {
        var original = new BloomFilterSerializer();
        var builder = TypeSerializerRegistry.builder().register(IndexType.BLOOM_FILTER, original);

        assertThatThrownBy(() -> builder.register(IndexType.BLOOM_FILTER, new BloomFilterSerializer()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate TypeSerializer");

        assertThat(builder.build().get(IndexType.BLOOM_FILTER)).isSameAs(original);
    }
}
