package org.pangrova.indexing.api.eval;

import org.pangrova.indexing.api.descriptor.IndexType;

import java.util.Objects;

/**
 * Lightweight descriptor carrying only the metadata needed by worker-side
 * index evaluation.
 *
 * <p>Distinct from {@link org.pangrova.indexing.api.descriptor.IndexDescriptor}, which is the
 * full metadata record managed by the coordinator and persisted in the
 * metadata store. Index evaluation only requires three pieces of information:
 * the index identity, its type (for engine dispatch), and the generation (for
 * cache keying). It does <strong>not</strong> need catalog/schema/table or
 * column names because the predicate already references columns by position
 * inside the serialized index payload.</p>
 *
 * <p>Constructing a partial {@code IndexDescriptor} with sentinel values like
 * {@code "_eval"} for the unknown fields (the previous worker pattern) leaks
 * placeholder identifiers into logs, metrics, and downstream code paths. This
 * record replaces that pattern with a type whose fields are exactly what the
 * worker has and uses.</p>
 *
 * @param indexId    index identifier
 * @param indexType  index type for engine lookup
 * @param generation index generation (cache key component)
 */
public record IndexEvalDescriptor(
        String indexId,
        IndexType indexType,
        int generation) {

    public IndexEvalDescriptor {
        Objects.requireNonNull(indexId, "indexId");
        Objects.requireNonNull(indexType, "indexType");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative: " + generation);
        }
    }

    /** Convenience factory: derive an eval descriptor from an {@link IndexEvalHint}. */
    public static IndexEvalDescriptor fromHint(IndexEvalHint hint) {
        Objects.requireNonNull(hint, "hint");
        return new IndexEvalDescriptor(hint.indexId(), hint.indexType(), hint.generation());
    }
}
