package org.pangrova.indexing.api.pruning;

import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;

import java.util.Objects;

/**
 * A hint from the planner indicating that a specific index can be used
 * to evaluate a predicate during split filtering.
 *
 * <p>Immutable. Created by IndexPredicateAnalyzer, consumed by
 * IndexSplitFilter in the coordinator.</p>
 *
 * @param descriptor immutable descriptor selected for the authorized table
 * @param predicate the predicate to evaluate against the index
 */
@org.pangrova.common.api.annotation.Experimental
public record IndexHint(IndexDescriptor descriptor, IndexPredicate predicate) {

    public IndexHint {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        if (descriptor.generation() < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        Objects.requireNonNull(predicate, "predicate must not be null");
    }

    public String indexId() { return descriptor.indexId(); }

    public IndexType indexType() { return descriptor.indexType(); }

    public int generation() { return descriptor.generation(); }
}
