package org.pangrova.indexing.api.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexType;

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
 * <p>Ownership is specified by each implementation. In particular, Bloom data
 * accessors return borrowed arrays which callers must not modify; constructor
 * copies do not make those views deeply immutable. Implementations shared by
 * concurrent evaluators must support concurrent reads under their ownership contract.</p>
 *
 * <p><strong>Known implementations (9):</strong>
 * {@link BTreeIndexData}, {@link BloomFilterIndexData}, {@link HashIndexData},
 * {@link org.pangrova.indexing.api.data.text.NGramBloomIndexData NGramBloomIndexData},
 * {@link org.pangrova.indexing.api.data.text.GinIndexData GinIndexData},
 * {@link org.pangrova.indexing.api.data.text.PrefixIndexData PrefixIndexData},
 * {@link org.pangrova.indexing.api.data.bitmap.BitmapIndexData BitmapIndexData},
 * {@link org.pangrova.indexing.api.data.spatial.RTreeIndexData RTreeIndexData},
 * {@link org.pangrova.indexing.api.data.spatial.SparseIndexData SparseIndexData}.</p>
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
