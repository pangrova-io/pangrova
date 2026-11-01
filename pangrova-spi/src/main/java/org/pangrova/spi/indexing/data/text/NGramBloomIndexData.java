package org.pangrova.spi.indexing.data.text;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.IndexData;

/**
 * N-gram bloom filter index data for substring (LIKE '%abc%') matching.
 * Strings are decomposed into character n-grams and inserted into a
 * bloom filter.
 *
 * @param filterData       the raw bloom filter byte array
 * @param numHashFunctions number of hash functions used
 * @param numBits          total number of bits in the filter
 * @param ngramSize        the n-gram size used during construction
 */
@Experimental
public record NGramBloomIndexData(
        byte[] filterData,
        int numHashFunctions,
        long numBits,
        int ngramSize) implements IndexData {

    public NGramBloomIndexData {
        if (filterData == null) {
            throw new IllegalArgumentException("filterData must not be null");
        }
        filterData = filterData.clone();
    }

    /**
     * Returns the internal filter byte array without copying.
     * Callers MUST NOT mutate the returned array.
     *
     * <p>The constructor performs a defensive copy to prevent external mutation
     * after construction. This accessor returns the internal array directly
     * to avoid MB-scale allocations on the hot evaluate path.
     */
    @Override
    public byte[] filterData() {
        return filterData;
    }

    @Override
    public IndexType indexType() {
        return IndexType.NGRAM_BLOOM_FILTER;
    }

    @Override
    public long estimatedSizeInBytes() {
        return filterData.length;
    }
}
