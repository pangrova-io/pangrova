package org.pangrova.spi.indexing.eval;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;

import java.util.Objects;

/**
 * Lightweight hint attached to a split by the coordinator, instructing the
 * worker to perform L2 index evaluation before reading data.
 *
 * <p>Contains the minimum information needed for worker-side evaluation:
 * the index identity, the predicate to evaluate, and the index file path
 * for this specific data file.</p>
 *
 * @param indexId       index identifier
 * @param indexType     index type (for engine lookup)
 * @param predicate     predicate to evaluate
 * @param indexFilePath path to the index file for this data file
 * @param generation    index generation (for cache key)
 */
@Experimental
public record IndexEvalHint(
        String indexId,
        IndexType indexType,
        IndexPredicate predicate,
        String indexFilePath,
        int generation) {

    public IndexEvalHint {
        Objects.requireNonNull(indexId, "indexId");
        Objects.requireNonNull(indexType, "indexType");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(indexFilePath, "indexFilePath");
    }
}
