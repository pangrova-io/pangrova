package org.pangrova.indexing.storage;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.BloomFilterIndexData;
import org.pangrova.spi.indexing.data.IndexData;

/**
 * Cache-aware wrapper that carries a pre-deserialized {@link SplitBlockBloomFilter}
 * alongside the original {@link BloomFilterIndexData}.
 *
 * <p>Created by {@link IndexCache#put} at cache insertion time to eliminate
 * per-evaluation deserialization ({@code SplitBlockBloomFilter.readFrom()}) on the
 * hot query path. The wrapper delegates all {@link IndexData} methods to the
 * underlying {@code BloomFilterIndexData}.</p>
 *
 * <p>Lives in {@code pangrova-indexing} (not a shared contract artifact) to avoid leaking
 * implementation types into the published SPI surface.</p>
 *
 * <p>Immutable and thread-safe: both the underlying record and the deserialized
 * filter are immutable after construction.</p>
 */
@ThreadSafe
public final class CachedBloomFilterIndexData implements IndexData {

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
        return delegate.estimatedSizeInBytes();
    }
}
