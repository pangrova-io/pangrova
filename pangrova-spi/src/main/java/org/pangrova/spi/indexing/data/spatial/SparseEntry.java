package org.pangrova.spi.indexing.data.spatial;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A single entry in a sparse index: a sampled key value and
 * the row offset where it was sampled.
 *
 * @param key       the sampled key value
 * @param rowOffset the row offset of this entry
 */
@Experimental
public record SparseEntry(Comparable<?> key, long rowOffset) {
}
