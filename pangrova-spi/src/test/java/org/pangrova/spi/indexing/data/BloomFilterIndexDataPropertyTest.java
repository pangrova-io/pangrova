package org.pangrova.spi.indexing.data;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import org.pangrova.spi.indexing.descriptor.IndexType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link BloomFilterIndexData} immutability and accessor correctness.
 *
 * <p>Validates: BF1 — accessor returns internal array without clone for zero-copy hot-path,
 * while construction-time defensive copy prevents external mutation of the source.</p>
 */
class BloomFilterIndexDataPropertyTest {

    @Property(tries = 10)
    void constructorDefensivelyCopiesInput(
            @ForAll @Size(min = 1, max = 128) byte[] input,
            @ForAll @IntRange(min = 1, max = 8) int numHash) {
        byte[] original = input.clone();
        var data = new BloomFilterIndexData(input, numHash, input.length * 8L);

        // Mutate the original — should not affect the record
        input[0] = (byte) (input[0] ^ 0xFF);
        assertThat(data.filterData()).isEqualTo(original);
    }

    @Property(tries = 10)
    void accessorReturnsStableReference(
            @ForAll @Size(min = 1, max = 128) byte[] input,
            @ForAll @IntRange(min = 1, max = 8) int numHash) {
        var data = new BloomFilterIndexData(input.clone(), numHash, input.length * 8L);

        // Two calls return the same array instance (zero-copy)
        byte[] first = data.filterData();
        byte[] second = data.filterData();
        assertThat(first).isSameAs(second);
    }

    @Property(tries = 10)
    void indexTypeIsBloomFilter(
            @ForAll @Size(min = 1, max = 32) byte[] input) {
        var data = new BloomFilterIndexData(input.clone(), 3, input.length * 8L);
        assertThat(data.indexType()).isEqualTo(IndexType.BLOOM_FILTER);
    }

    @Property(tries = 10)
    void estimatedSizeMatchesArrayLength(
            @ForAll @Size(min = 1, max = 256) byte[] input) {
        var data = new BloomFilterIndexData(input.clone(), 3, input.length * 8L);
        assertThat(data.estimatedSizeInBytes()).isEqualTo(input.length);
    }
}
