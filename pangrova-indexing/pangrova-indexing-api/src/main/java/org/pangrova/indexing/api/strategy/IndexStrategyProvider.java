package org.pangrova.indexing.api.strategy;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.indexing.api.codec.TypeSerializer;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.pangrova.connector.api.enums.IndexEvalResult;

/**
 * SPI contract for pluggable index strategies.
 *
 * <p>Each implementation handles one {@link IndexType} and provides both
 * build and evaluate capabilities. Implementations are discovered via
 * {@link java.util.ServiceLoader}.
 */
@Experimental
public interface IndexStrategyProvider {

    /**
     * Returns the index type this provider handles.
     *
     * @return the supported index type (never null)
     */
    IndexType supportedType();

    /** Codec for the raw payload built by this strategy. The caller owns the common artifact envelope. */
    TypeSerializer<?> payloadSerializer();

    /**
     * Builds index data from the given column values.
     *
     * @param values     iterator over rows, each row is a list of column values
     * @param properties index configuration properties (e.g. page size, analyzer)
     * @return the built index data
     * @throws IllegalArgumentException if properties are invalid for this index type
     */
    IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties);

    /**
     * Prepares loaded data for repeated evaluation by this provider.
     *
     * <p>The default is identity: preparation is an optional optimization, not
     * a substitute for missing provider support. The result must be non-null,
     * retain the same index type and evaluation semantics, and be safe for
     * concurrent read-only evaluations. Neither this method nor evaluation may
     * mutate the borrowed input. The caller retains the result until eviction;
     * no external resource may require closing. Its estimated size must include
     * all additionally retained buffers. Prepared data is not a wire payload and
     * must not be passed to a payload serializer.</p>
     *
     * @throws IllegalArgumentException if the data is invalid for this provider
     */
    default IndexData prepareForEvaluation(IndexData data) {
        return java.util.Objects.requireNonNull(data, "data");
    }

    /**
     * Evaluates whether a data file may contain rows matching the predicate,
     * based on the provided index data.
     *
     * <p>The evaluation contract guarantees no false negatives: if the data
     * file contains matching rows, the result is never {@link IndexEvalResult#SKIP}.
     *
     * @param indexData the index data for the target data file
     * @param predicate the predicate to evaluate
     * @return the evaluation result
     */
    IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate);

}
