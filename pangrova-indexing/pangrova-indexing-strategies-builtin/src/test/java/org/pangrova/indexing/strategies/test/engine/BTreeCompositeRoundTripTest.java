package org.pangrova.indexing.strategies.test.engine;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.connector.api.index.predicate.IsNull;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.indexing.api.data.BTreeIndexData;
import org.pangrova.indexing.api.data.CompositeIndexKey;
import org.pangrova.indexing.api.data.BTreePage;
import org.pangrova.indexing.strategies.btree.provider.BTreeStrategyProvider;
import org.pangrova.indexing.strategies.btree.serializer.BTreeSerializer;
import org.pangrova.indexing.codec.data.IndexSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.UncheckedIOException;
import org.pangrova.indexing.codec.data.SerdeCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BTreeCompositeRoundTripTest {
    @Test
    void builtCompositeKeysSurviveTheProductionEnvelope() {
        var provider = new BTreeStrategyProvider();
        List<List<Comparable<?>>> rows = List.of(List.of(2L, "b"), List.of(1L, "a"), List.of(2L, "a"));
        var data = (BTreeIndexData) provider.build(rows.iterator(),
                Map.of("btree.page_size", "2", "btree.store_positions", "true"));
        var serializer = new IndexSerializer(TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class)));
        var restored = serializer.deserialize(serializer.serialize(data)).orElseThrow();
        assertThat(restored).isEqualTo(data);
        assertThat(data.leafPages().get(0).rowPositions()).containsExactly(List.of(1), List.of(2));
        assertThat(provider.evaluate(restored, new EqualTo("bound-key", new CompositeIndexKey(List.of(2L, "a")))))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(restored, new EqualTo("bound-key", new CompositeIndexKey(List.of(2L, "missing")))))
                .isEqualTo(IndexEvalResult.SKIP);
        assertThat(provider.evaluate(restored, new EqualTo("a", 2L))).isEqualTo(IndexEvalResult.UNKNOWN);
        assertThat(provider.evaluate(restored, new InList("a", List.of(9L, 2L)))).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void omittedNullRowsCannotProveIsNullAbsent() {
        var provider = new BTreeStrategyProvider();
        List<List<Comparable<?>>> rows = List.of(Arrays.asList((Comparable<?>) null));
        var data = provider.build(rows.iterator(), Map.of());
        assertThat(provider.evaluate(data, new IsNull("a"))).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void partialNullRowsCannotProveAScalarPredicateAbsent() {
        var provider = new BTreeStrategyProvider();
        for (List<List<Comparable<?>>> rows : List.<List<List<Comparable<?>>>>of(
                List.of(Arrays.asList(5L, null)), List.of(Arrays.asList(5L, null), List.of(2L, "b")))) {
            var data = provider.build(rows.iterator(), Map.of());
            assertThat(provider.evaluate(data, new EqualTo("a", 5L))).isEqualTo(IndexEvalResult.UNKNOWN);
        }
    }

    @Test
    void rejectsInvalidBuildShapeAndPageSizeBeforeProducingAnArtifact() {
        var provider = new BTreeStrategyProvider();
        List<List<Comparable<?>>> rows = List.of(List.of(1L, "a"));
        for (String size : List.of("0", "-1")) {
            assertThatThrownBy(() -> provider.build(rows.iterator(), Map.of("btree.page_size", size)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat((BTreeIndexData) provider.build(rows.iterator(), Map.of("btree.page_size", "2147483647")))
                .extracting(data -> data.leafPages().size()).isEqualTo(1);
        List<List<Comparable<?>>> uneven = List.of(List.of(1L, "a"), List.of(2L));
        assertThatThrownBy(() -> provider.build(uneven.iterator(), Map.of())).isInstanceOf(IllegalArgumentException.class);
        List<List<Comparable<?>>> emptyRow = List.of(List.of());
        assertThatThrownBy(() -> provider.build(emptyRow.iterator(), Map.of())).isInstanceOf(IllegalArgumentException.class);
        List<List<Comparable<?>>> incompatible = List.of(List.of(1L, "a"), List.of(2, "b"));
        assertThatThrownBy(() -> provider.build(incompatible.iterator(), Map.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsImpossiblePayloadCountsTrailingBytesAndMissingPositions() {
        var serializer = new BTreeSerializer();
        for (int count : new int[]{-1, Integer.MAX_VALUE}) {
            byte[] bytes = SerdeCodec.toBytes(out -> { out.writeBoolean(false); out.writeInt(count); });
            assertThatThrownBy(() -> serializer.deserialize(bytes)).isInstanceOf(UncheckedIOException.class);
        }
        byte[] empty = serializer.serialize(new BTreeIndexData(List.of()));
        assertThatThrownBy(() -> serializer.deserialize(Arrays.copyOf(empty, empty.length + 1)))
                .isInstanceOf(UncheckedIOException.class);
        var malformed = new BTreeIndexData(List.of(new BTreePage(List.of(1L), null, 1L, 1L)), true);
        assertThatThrownBy(() -> serializer.serialize(malformed)).isInstanceOf(IllegalArgumentException.class);
    }
}
