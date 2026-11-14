package org.pangrova.indexing.api.data;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import org.pangrova.indexing.api.enums.IndexType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link BloomFilterIndexData} immutability and accessor correctness.
 *
 * <p>Construction and snapshots isolate mutation; read-only views expose encoded
 * data without granting write access.</p>
 */
class BloomFilterIndexDataPropertyTest {

    @Property(tries = 10, seed = "20261103")
    void constructorDefensivelyCopiesInput(
            @ForAll @Size(min = 1, max = 128) byte[] input,
            @ForAll @IntRange(min = 1, max = 8) int numHash) {
        byte[] original = input.clone();
        var data = new BloomFilterIndexData(input, numHash, input.length * 8L);

        // Mutate the original — should not affect the record
        input[0] = (byte) (input[0] ^ 0xFF);
        assertThat(data.filterData()).isEqualTo(original);
    }

    @Property(tries = 10, seed = "20261103")
    void accessorMutationCannotChangeIndex(
            @ForAll @Size(min = 1, max = 128) byte[] input,
            @ForAll @IntRange(min = 1, max = 8) int numHash) {
        var data = new BloomFilterIndexData(input.clone(), numHash, input.length * 8L);

        byte[] snapshot = data.filterData();
        snapshot[0] ^= (byte) 0xFF;
        assertThat(data.filterData()).isEqualTo(input);
        assertThat(data.filterDataView().get(0)).isEqualTo(input[0]);
    }

    @Property(tries = 10, seed = "20261103")
    void indexTypeIsBloomFilter(
            @ForAll @Size(min = 1, max = 32) byte[] input) {
        var data = new BloomFilterIndexData(input.clone(), 3, input.length * 8L);
        assertThat(data.indexType()).isEqualTo(IndexType.BLOOM_FILTER);
    }

    @Property(tries = 10, seed = "20261103")
    void estimatedSizeMatchesArrayLength(
            @ForAll @Size(min = 1, max = 256) byte[] input) {
        var data = new BloomFilterIndexData(input.clone(), 3, input.length * 8L);
        assertThat(data.estimatedSizeInBytes()).isEqualTo(input.length);
    }
}
