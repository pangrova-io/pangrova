package org.pangrova.indexing.storage;

import org.pangrova.indexing.builtin.bloom.data.CachedBloomFilterIndexData;
import org.pangrova.indexing.builtin.bloom.provider.BloomFilterStrategyProvider;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CachedBloomFilterIndexData} wrapper and
 * provider preparation followed by {@link IndexCache} insertion.
 */
class CachedBloomFilterIndexDataTest {

    @Test
    void wrapperDelegatesToUnderlyingRecord() {
        SplitBlockBloomFilter filter = buildFilter("hello", "world");
        byte[] serialized = filter.toByteArray();
        BloomFilterIndexData delegate = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);

        CachedBloomFilterIndexData cached = new CachedBloomFilterIndexData(delegate, filter);

        assertThat(cached.indexType()).isEqualTo(IndexType.BLOOM_FILTER);
        assertThat(cached.estimatedSizeInBytes()).isGreaterThanOrEqualTo(delegate.estimatedSizeInBytes() + filter.sizeInBytes());
        assertThat(cached.delegate()).isSameAs(delegate);
        assertThat(cached.filter()).isSameAs(filter);
        assertThat(cached.filterData()).isEqualTo(delegate.filterData());
    }

    @Test
    void nullDelegateThrows() {
        SplitBlockBloomFilter filter = buildFilter("x");
        assertThatThrownBy(() -> new CachedBloomFilterIndexData(null, filter))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullFilterThrows() {
        byte[] serialized = buildFilter("x").toByteArray();
        BloomFilterIndexData delegate = new BloomFilterIndexData(serialized, 8, 256);
        assertThatThrownBy(() -> new CachedBloomFilterIndexData(delegate, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void providerPreparesBloomFilterBeforeCacheInsertion() {
        IndexCache cache = new IndexCache(1024 * 1024);
        SplitBlockBloomFilter filter = buildFilter("alpha", "beta", "gamma");
        byte[] serialized = filter.toByteArray();
        BloomFilterIndexData original = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);

        cache.put("idx-1", "file-1", 0, new BloomFilterStrategyProvider().prepareForEvaluation(original));
        IndexData retrieved = cache.get("idx-1", "file-1", 0);

        assertThat(retrieved).isInstanceOf(CachedBloomFilterIndexData.class);
        CachedBloomFilterIndexData cached = (CachedBloomFilterIndexData) retrieved;
        assertThat(cached.delegate()).isEqualTo(original);
        assertThat(cached.filter().mightContain("alpha")).isTrue();
        assertThat(cached.filter().mightContain("beta")).isTrue();
        assertThat(cached.filter().mightContain("gamma")).isTrue();
        assertThat(cached.filter().mightContain("delta")).isFalse();
    }

    @Test
    void providerPreparationDoesNotDoubleWrap() {
        IndexCache cache = new IndexCache(1024 * 1024);
        SplitBlockBloomFilter filter = buildFilter("x");
        byte[] serialized = filter.toByteArray();
        BloomFilterIndexData delegate = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);
        CachedBloomFilterIndexData alreadyCached = new CachedBloomFilterIndexData(delegate, filter);

        cache.put("idx-1", "file-1", 0, new BloomFilterStrategyProvider().prepareForEvaluation(alreadyCached));
        IndexData retrieved = cache.get("idx-1", "file-1", 0);

        assertThat(retrieved).isSameAs(alreadyCached);
    }

    @Test
    void providerRejectsMalformedBloomDataBeforeCaching() {
        BloomFilterIndexData malformed = new BloomFilterIndexData(new byte[]{1, 2, 3}, 3, 24);
        assertThatThrownBy(() -> new BloomFilterStrategyProvider().prepareForEvaluation(malformed))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void evictionHandlesCachedBloomFilterCorrectly() {
        // Cache with very small max size to force eviction
        IndexCache cache = new IndexCache(200);
        SplitBlockBloomFilter filter = buildFilter("val");
        byte[] serialized = filter.toByteArray();

        // Insert enough entries to trigger eviction
        for (int i = 0; i < 10; i++) {
            BloomFilterIndexData data = new BloomFilterIndexData(serialized, 8, filter.numBlocks() * 256L);
            cache.put("idx-" + i, "file-" + i, 0, new BloomFilterStrategyProvider().prepareForEvaluation(data));
        }

        // Cache should respect max size — some entries evicted
        assertThat(cache.currentSizeBytes()).isLessThanOrEqualTo(200);
    }

    private static SplitBlockBloomFilter buildFilter(String... values) {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(100, 0.01);
        for (String v : values) {
            builder.put(v);
        }
        return builder.build();
    }
}
