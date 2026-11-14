package org.pangrova.indexing.api.data.text;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.IndexData;

import java.nio.ByteBuffer;

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
        return IndexType.NGRAM_BLOOM_FILTER;
    }

    @Override
    public long estimatedSizeInBytes() {
        return filterData.length;
    }
}
