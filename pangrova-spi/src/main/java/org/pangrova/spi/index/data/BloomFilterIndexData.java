package org.pangrova.spi.index.data;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;

/**
 * Bloom filter index data for probabilistic membership testing.
 *
 * @param filterData       the raw bloom filter byte array
 * @param numHashFunctions number of hash functions used
 * @param numBits          total number of bits in the filter
 */
@Experimental
public record BloomFilterIndexData(
        byte[] filterData,
        int numHashFunctions,
        long numBits) implements IndexData {

    public BloomFilterIndexData {
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
        return IndexType.BLOOM_FILTER;
    }

    @Override
    public long estimatedSizeInBytes() {
        return filterData.length;
    }
}
