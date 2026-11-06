package org.pangrova.indexing.builtin.test.engine.bloom.provider;

import org.pangrova.indexing.builtin.bloom.provider.BloomFilterStrategyProvider;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.indexing.builtin.bloom.data.CachedBloomFilterIndexData;
import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.HashIndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BloomFilterStrategyProvider}, verifying both
 * cached (via {@link CachedBloomFilterIndexData}) and uncached evaluation paths.
 */
class BloomFilterStrategyProviderTest {

    private final BloomFilterStrategyProvider provider = new BloomFilterStrategyProvider();

    @Test
    void evaluateWithCachedFilterReturnsKeepForInsertedValue() {
        CachedBloomFilterIndexData cached = buildCachedData("hello", "world");

        IndexEvalResult result = provider.evaluate(cached, new EqualTo("col", "hello"));

        assertThat(result).isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void evaluateWithCachedFilterReturnsSkipForAbsentValue() {
        CachedBloomFilterIndexData cached = buildCachedData("hello", "world");

        IndexEvalResult result = provider.evaluate(cached, new EqualTo("col", "missing"));

        assertThat(result).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateWithUncachedBloomFilterStillWorks() {
        SplitBlockBloomFilter filter = buildFilter("alpha", "beta");
        byte[] serialized = filter.toByteArray();
        BloomFilterIndexData uncached = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);

        assertThat(provider.evaluate(uncached, new EqualTo("col", "alpha")))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(uncached, new EqualTo("col", "missing")))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateInListWithCachedFilter() {
        CachedBloomFilterIndexData cached = buildCachedData("a", "b", "c");

        assertThat(provider.evaluate(cached, new InList("col", List.of("b", "x"))))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(cached, new InList("col", List.of("x", "y"))))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateReturnsUnknownForNonBloomData() {
        HashIndexData hash = new HashIndexData(new byte[]{1}, 0, 1);

        assertThat(provider.evaluate(hash, new EqualTo("col", "x")))
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void supportedTypeIsBloomFilter() {
        assertThat(provider.supportedType()).isEqualTo(IndexType.BLOOM_FILTER);
    }

    private static CachedBloomFilterIndexData buildCachedData(String... values) {
        SplitBlockBloomFilter filter = buildFilter(values);
        byte[] serialized = filter.toByteArray();
        BloomFilterIndexData delegate = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);
        return new CachedBloomFilterIndexData(delegate, filter);
    }

    private static SplitBlockBloomFilter buildFilter(String... values) {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(100, 0.01);
        for (String v : values) {
            builder.put(v);
        }
        return builder.build();
    }
}
