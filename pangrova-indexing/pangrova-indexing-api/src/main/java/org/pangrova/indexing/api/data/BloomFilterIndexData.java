package org.pangrova.indexing.api.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexType;

import java.nio.ByteBuffer;

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

    /** Returns an owned snapshot; mutations never affect this index. */
    @Override
    public byte[] filterData() {
        return filterData.clone();
    }

    /**
     * Returns a read-only view without copying the encoded bytes. Each view has
     * an independent cursor and cannot expose the backing array. Decode through
     * this view on evaluation paths; use {@link #filterData()} for an owned copy.
     */
    public ByteBuffer filterDataView() {
        return ByteBuffer.wrap(filterData).asReadOnlyBuffer();
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
