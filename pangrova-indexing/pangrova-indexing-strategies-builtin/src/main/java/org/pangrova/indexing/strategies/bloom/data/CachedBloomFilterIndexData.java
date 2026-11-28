package org.pangrova.indexing.strategies.bloom.data;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;

/**
 * Cache-aware wrapper that carries a pre-deserialized {@link SplitBlockBloomFilter}
 * alongside the original {@link BloomFilterIndexData}.
 *
 * <p>Created by the Bloom strategy before evaluation and cache insertion.
 * Both serialized bytes and the separately allocated parsed array are counted
 * in the retained-size estimate. This implementation type never crosses the
 * wire or the published SPI signature.</p>
 *
 * <p>Immutable and thread-safe: both the underlying record and the deserialized
 * filter are immutable after construction.</p>
 */
@ThreadSafe
public final class CachedBloomFilterIndexData implements IndexData {

    // Coarse allowance for the wrapper, filter, record and both array headers.
    // Buffer payloads are counted separately; this is not a portable JVM layout measurement.
    private static final long OBJECT_OVERHEAD_ESTIMATE = 160L;

    private final BloomFilterIndexData delegate;
    private final SplitBlockBloomFilter deserializedFilter;

    /** Wraps the serialized record together with its pre-deserialized filter. */
    public CachedBloomFilterIndexData(BloomFilterIndexData delegate,
                                      SplitBlockBloomFilter deserializedFilter) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        if (deserializedFilter == null) {
            throw new IllegalArgumentException("deserializedFilter must not be null");
        }
        this.delegate = delegate;
        this.deserializedFilter = deserializedFilter;
    }

    /** Returns the pre-deserialized bloom filter for zero-allocation evaluation. */
    public SplitBlockBloomFilter filter() {
        return deserializedFilter;
    }

    /** Returns the underlying SPI record. */
    public BloomFilterIndexData delegate() {
        return delegate;
    }

    /** Returns the raw filter bytes from the underlying record. */
    public byte[] filterData() {
        return delegate.filterData();
    }

    @Override
    public IndexType indexType() {
        return delegate.indexType();
    }

    @Override
    public long estimatedSizeInBytes() {
        return delegate.estimatedSizeInBytes() + deserializedFilter.sizeInBytes() + OBJECT_OVERHEAD_ESTIMATE;
    }
}
