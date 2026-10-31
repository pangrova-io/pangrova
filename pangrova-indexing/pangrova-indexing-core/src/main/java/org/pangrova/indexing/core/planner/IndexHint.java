package org.pangrova.indexing.core.planner;

import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.connector.spi.index.predicate.IndexPredicate;

import java.util.Objects;

/**
 * A hint from the planner indicating that a specific index can be used
 * to evaluate a predicate during split filtering.
 *
 * <p>Immutable. Created by IndexPredicateAnalyzer, consumed by
 * IndexSplitFilter in the coordinator.</p>
 *
 * @param indexId   the index to use
 * @param indexType type of the index (for engine lookup)
 * @param predicate the predicate to evaluate against the index
 */
public record IndexHint(
        String indexId,
        IndexType indexType,
        IndexPredicate predicate) {

    public IndexHint {
        Objects.requireNonNull(indexId, "indexId must not be null");
        Objects.requireNonNull(indexType, "indexType must not be null");
        Objects.requireNonNull(predicate, "predicate must not be null");
    }
}
