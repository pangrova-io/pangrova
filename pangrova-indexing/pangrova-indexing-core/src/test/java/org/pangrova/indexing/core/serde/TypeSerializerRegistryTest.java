package org.pangrova.indexing.core.serde;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.engine.bloom.serializer.BloomFilterSerializer;

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
