package org.pangrova.indexing.strategies.test.property.engine;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.indexing.api.data.CompositeIndexKey;
import org.pangrova.indexing.strategies.btree.provider.BTreeStrategyProvider;
import org.pangrova.indexing.codec.data.IndexSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BTreeCompositePropertyTest {
    @Property(tries = 100, seed = "20261103")
    void storedTupleMembershipHasNoFalseNegativesAcrossPageSizes(
            @ForAll @IntRange(min = 1, max = 70) int count,
            @ForAll @IntRange(min = 1, max = 10) int pageSize) {
        var rows = new ArrayList<List<Comparable<?>>>();
        for (int i = count; i > 0; i--) rows.add(List.of((long) (i % 5), "v" + (i % 9)));
        var provider = new BTreeStrategyProvider();
        var data = provider.build(rows.iterator(), Map.of("btree.page_size", Integer.toString(pageSize)));
        var serializer = new IndexSerializer(TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class)));
        var restored = serializer.deserialize(serializer.serialize(data)).orElseThrow();
        for (List<Comparable<?>> row : rows) {
            assertThat(provider.evaluate(restored, new EqualTo("bound-key", new CompositeIndexKey(row))))
                    .isEqualTo(IndexEvalResult.KEEP);
        }
        assertThat(provider.evaluate(restored, new EqualTo("bound-key", new CompositeIndexKey(List.of(9L, "absent")))))
                .isEqualTo(IndexEvalResult.SKIP);
    }
}
