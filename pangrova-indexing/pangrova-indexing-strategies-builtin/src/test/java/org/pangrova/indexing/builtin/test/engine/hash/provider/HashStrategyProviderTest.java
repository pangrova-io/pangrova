package org.pangrova.indexing.builtin.test.engine.hash.provider;

import org.pangrova.indexing.builtin.hash.provider.HashStrategyProvider;

import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.HashIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HashStrategyProvider}, verifying incremental
 * hash table build with probe-on-insert dedup and evaluation correctness.
 */
class HashStrategyProviderTest {

    private final HashStrategyProvider provider = new HashStrategyProvider();

    @Test
    void supportedTypeIsHash() {
        assertThat(provider.supportedType()).isEqualTo(IndexType.HASH);
    }

    @Test
    void buildAndEvaluateEqualToKeepsInsertedValue() {
        IndexData data = buildIndex("alpha", "beta", "gamma");

        assertThat(provider.evaluate(data, new EqualTo("col", "alpha")))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(data, new EqualTo("col", "beta")))
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void buildAndEvaluateEqualToSkipsAbsentValue() {
        IndexData data = buildIndex("alpha", "beta");

        assertThat(provider.evaluate(data, new EqualTo("col", "missing")))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void buildDeduplicatesDuplicateValues() {
        IndexData data = buildIndex("x", "x", "x", "y", "y", "z");
        HashIndexData hashData = (HashIndexData) data;

        assertThat(hashData.entryCount()).isEqualTo(3);
        assertThat(provider.evaluate(data, new EqualTo("col", "x")))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(data, new EqualTo("col", "y")))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(data, new EqualTo("col", "z")))
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void buildHandlesNullAndEmptyRows() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        rows.add(List.of("valid"));
        rows.add(List.of());                    // empty row
        rows.add(Collections.singletonList(null)); // null value
        rows.add(List.of("another"));

        IndexData data = provider.build(rows.iterator(), Map.of());
        HashIndexData hashData = (HashIndexData) data;

        assertThat(hashData.entryCount()).isEqualTo(2);
    }

    @Test
    void buildTriggersResizeForManyDistinctValues() {
        // Insert enough distinct values to force multiple resizes
        // (initial capacity=16, load factor=0.7 → resize at 11 entries)
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add("value_" + i);
        }
        IndexData data = buildIndex(values.toArray(new String[0]));
        HashIndexData hashData = (HashIndexData) data;

        assertThat(hashData.entryCount()).isEqualTo(100);
        // Verify all values are findable after resizes
        for (String v : values) {
            assertThat(provider.evaluate(data, new EqualTo("col", v)))
                    .as("Expected KEEP for %s", v)
                    .isEqualTo(IndexEvalResult.KEEP);
        }
    }

    @Test
    void evaluateInListKeepsWhenAnyValuePresent() {
        IndexData data = buildIndex("a", "b", "c");

        assertThat(provider.evaluate(data, new InList("col", List.of("b", "x"))))
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void evaluateInListSkipsWhenNoValuePresent() {
        IndexData data = buildIndex("a", "b", "c");

        assertThat(provider.evaluate(data, new InList("col", List.of("x", "y"))))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateReturnsUnknownForNonHashData() {
        BloomFilterIndexData bloom = new BloomFilterIndexData(new byte[]{1}, 8, 256);

        assertThat(provider.evaluate(bloom, new EqualTo("col", "x")))
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void buildWithEmptyIteratorProducesEmptyIndex() {
        Iterator<List<Comparable<?>>> empty = Collections.emptyIterator();
        IndexData data = provider.build(empty, Map.of());
        HashIndexData hashData = (HashIndexData) data;

        assertThat(hashData.entryCount()).isZero();
    }

    @Test
    void buildWithDuplicatesAcrossResizeBoundary() {
        // Mix duplicates with enough distinct values to trigger resize,
        // verifying dedup works correctly across rehash boundaries
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(List.of((Comparable<?>) ("val_" + (i % 20))));
        }
        IndexData data = provider.build(rows.iterator(), Map.of());
        HashIndexData hashData = (HashIndexData) data;

        assertThat(hashData.entryCount()).isEqualTo(20);
    }

    private IndexData buildIndex(String... values) {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (String v : values) {
            rows.add(List.of((Comparable<?>) v));
        }
        return provider.build(rows.iterator(), Map.of());
    }
}
