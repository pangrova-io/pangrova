package org.pangrova.indexing.api.pruning;

import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

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
 * @param generation generation observed during predicate analysis
 * @param predicate the predicate to evaluate against the index
 */
@org.pangrova.common.api.annotation.Experimental
public record IndexHint(
        String indexId,
        IndexType indexType,
        int generation,
        IndexPredicate predicate) {

    public IndexHint {
        Objects.requireNonNull(indexId, "indexId must not be null");
        Objects.requireNonNull(indexType, "indexType must not be null");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        Objects.requireNonNull(predicate, "predicate must not be null");
    }
}
