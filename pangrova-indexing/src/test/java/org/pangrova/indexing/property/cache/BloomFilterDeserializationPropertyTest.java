package org.pangrova.indexing.property.cache;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.constraints.Size;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.indexing.storage.CachedBloomFilterIndexData;
import org.pangrova.spi.index.data.BloomFilterIndexData;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for BloomFilter deserialization equivalence.
 *
 * <p><b>Property P10: BloomFilter No False Negatives Preserved</b></p>
 *
 * <p>Validates that the cached deserialization path ({@link CachedBloomFilterIndexData})
 * produces identical {@code mightContain()} results to the per-evaluation deserialization
 * path ({@link SplitBlockBloomFilter#readFrom(byte[])}). This proves the cached path
 * preserves the no-false-negatives property.</p>
 *
 * <p><b>Validates: Requirements 1.AC1</b></p>
 */
class BloomFilterDeserializationPropertyTest {

    @Property(tries = 10)
    void insertedValuesDetectedByBothDeserializationPaths(
            @ForAll("distinctValues") List<String> values) {
        // Build filter with all values
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(
                Math.max(values.size(), 1), 0.01);
        for (String v : values) {
            builder.put(v);
        }
        SplitBlockBloomFilter original = builder.build();
        byte[] serialized = original.toByteArray();

        // Path 1: Pre-deserialized (cached path — what CachedBloomFilterIndexData does)
        SplitBlockBloomFilter preDeserialized = SplitBlockBloomFilter.readFrom(serialized);
        BloomFilterIndexData delegate = new BloomFilterIndexData(
                serialized, 8, (long) original.numBlocks() * 256);
        CachedBloomFilterIndexData cached = new CachedBloomFilterIndexData(delegate, preDeserialized);

        // Path 2: Per-eval deserialization (old code path — readFrom each time)
        SplitBlockBloomFilter perEvalDeserialized = SplitBlockBloomFilter.readFrom(
                delegate.filterData());

        // Assert: for ALL inserted values, both paths return true (no false negatives)
        for (String v : values) {
            assertThat(cached.filter().mightContain(v))
                    .as("Cached path must contain inserted value: %s", v)
                    .isTrue();
            assertThat(perEvalDeserialized.mightContain(v))
                    .as("Per-eval path must contain inserted value: %s", v)
                    .isTrue();
            assertThat(cached.filter().mightContain(v))
                    .as("Both paths must agree on inserted value: %s", v)
                    .isEqualTo(perEvalDeserialized.mightContain(v));
        }
    }

    @Property(tries = 10)
    void nonInsertedValuesAgreeAcrossDeserializationPaths(
            @ForAll("distinctValues") List<String> inserted,
            @ForAll("probeValues") List<String> probes) {
        // Build filter with inserted values
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(
                Math.max(inserted.size(), 1), 0.01);
        Set<String> insertedSet = new HashSet<>(inserted);
        for (String v : inserted) {
            builder.put(v);
        }
        SplitBlockBloomFilter original = builder.build();
        byte[] serialized = original.toByteArray();

        // Path 1: Pre-deserialized (cached path)
        SplitBlockBloomFilter preDeserialized = SplitBlockBloomFilter.readFrom(serialized);
        BloomFilterIndexData delegate = new BloomFilterIndexData(
                serialized, 8, (long) original.numBlocks() * 256);
        CachedBloomFilterIndexData cached = new CachedBloomFilterIndexData(delegate, preDeserialized);

        // Path 2: Per-eval deserialization
        SplitBlockBloomFilter perEvalDeserialized = SplitBlockBloomFilter.readFrom(
                delegate.filterData());

        // Assert: for non-inserted probe values, both paths return the same result
        for (String probe : probes) {
            boolean cachedResult = cached.filter().mightContain(probe);
            boolean perEvalResult = perEvalDeserialized.mightContain(probe);
            assertThat(cachedResult)
                    .as("Both paths must agree on probe value: %s", probe)
                    .isEqualTo(perEvalResult);
        }
    }

    @Provide
    Arbitrary<List<String>> distinctValues() {
        return Arbitraries.strings()
                .alpha().numeric()
                .ofMinLength(1).ofMaxLength(50)
                .list().ofMinSize(1).ofMaxSize(100)
                .uniqueElements();
    }

    @Provide
    Arbitrary<List<String>> probeValues() {
        return Arbitraries.strings()
                .alpha().numeric()
                .ofMinLength(1).ofMaxLength(50)
                .list().ofMinSize(1).ofMaxSize(50);
    }
}
