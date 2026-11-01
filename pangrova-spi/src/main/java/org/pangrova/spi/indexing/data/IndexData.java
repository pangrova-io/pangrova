package org.pangrova.spi.indexing.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.indexing.descriptor.IndexType;

/**
 * Opaque container for loaded index data. Each {@link IndexType} has its own
 * concrete record implementation.
 *
 * <p>Implementations reside in domain-specific sub-packages:
 * <ul>
 *   <li>{@code data} — BTree, BloomFilter, Hash (general-purpose)</li>
 *   <li>{@code data.text} — NGramBloom, Gin, Prefix (text pruning)</li>
 *   <li>{@code data.spatial} — RTree, Sparse, BoundingBox (spatial/range)</li>
 *   <li>{@code data.bitmap} — Bitmap (low-cardinality)</li>
 * </ul>
 *
 * <p>All implementations are immutable and thread-safe. Mutable fields
 * ({@code byte[]}, {@code List}, {@code Map}) are defensively copied on
 * construction.</p>
 *
 * <p><strong>Known implementations (9):</strong>
 * {@link BTreeIndexData}, {@link BloomFilterIndexData}, {@link HashIndexData},
 * {@link org.pangrova.spi.indexing.data.text.NGramBloomIndexData NGramBloomIndexData},
 * {@link org.pangrova.spi.indexing.data.text.GinIndexData GinIndexData},
 * {@link org.pangrova.spi.indexing.data.text.PrefixIndexData PrefixIndexData},
 * {@link org.pangrova.spi.indexing.data.bitmap.BitmapIndexData BitmapIndexData},
 * {@link org.pangrova.spi.indexing.data.spatial.RTreeIndexData RTreeIndexData},
 * {@link org.pangrova.spi.indexing.data.spatial.SparseIndexData SparseIndexData}.</p>
 */
@Experimental
public interface IndexData {

    /**
     * Returns the index type this data belongs to.
     */
    IndexType indexType();

    /**
     * Returns the approximate memory footprint in bytes.
     * Used by IndexCache for eviction decisions.
     */
    long estimatedSizeInBytes();
}
