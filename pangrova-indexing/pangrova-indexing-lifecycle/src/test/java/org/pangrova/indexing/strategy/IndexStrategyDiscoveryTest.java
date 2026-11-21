package org.pangrova.indexing.strategy;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexStrategyDiscoveryTest {
    @Test
    void productionDiscoveryBindsEveryScalarStrategyToItsPayloadCodec() {
        var strategies = new IndexEngineRegistry();
        var codecs = TypeSerializerRegistry.fromStrategies(strategies.providers());
        var serializer = new IndexSerializer(codecs);
        var scalarTypes = EnumSet.of(IndexType.BTREE, IndexType.BLOOM_FILTER, IndexType.PREFIX,
                IndexType.NGRAM_BLOOM_FILTER, IndexType.BITMAP, IndexType.HASH, IndexType.SPARSE,
                IndexType.R_TREE, IndexType.GIN);
        assertThat(strategies.providers()).extracting(IndexStrategyProvider::supportedType)
                .containsExactlyInAnyOrderElementsOf(scalarTypes);
        for (var type : scalarTypes) {
            var provider = strategies.getProvider(type);
            List<List<Comparable<?>>> rows = type == IndexType.R_TREE
                    ? List.of(List.of(1.0, 2.0)) : List.of(List.of("search"));
            var raw = provider.build(rows.iterator(), Map.of());
            byte[] encoded = serializer.serialize(raw);
            var restored = serializer.deserialize(encoded).orElseThrow();
            assertThat(restored.indexType()).isEqualTo(type);
            assertThat(serializer.serialize(restored)).containsExactly(encoded);
            assertThat(provider.prepareForEvaluation(restored).indexType()).isEqualTo(type);
        }
        for (var nativeType : List.of(IndexType.VECTOR, IndexType.INVERTED)) {
            assertThatThrownBy(() -> strategies.getProvider(nativeType)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> codecs.get(nativeType)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void installedStrategiesAreIndependentOfTheCallingThreadLoader() {
        var thread = Thread.currentThread();
        var original = thread.getContextClassLoader();
        var empty = new ClassLoader(null) {};
        try {
            thread.setContextClassLoader(empty);
            assertThat(new IndexEngineRegistry().size()).isEqualTo(9);
            assertThat(new IndexEngineRegistry(empty).size()).isZero();
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    @Test
    void duplicateStrategyCannotSupplyTwoCodecsForOneType() {
        var provider = new IndexEngineRegistry().getProvider(IndexType.HASH);
        var duplicate = List.of(provider, provider);
        assertThatThrownBy(() -> new IndexEngineRegistry(duplicate))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("HASH");
        assertThatThrownBy(() -> TypeSerializerRegistry.fromStrategies(duplicate))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("HASH");
    }
}
