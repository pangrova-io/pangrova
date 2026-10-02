package org.pangrova.spi.index;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.predicate.IndexPredicate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
     * Returns whether this provider supports incremental index updates.
     *
     * @return {@code true} if {@link #buildIncremental} is implemented
     */
    default boolean supportsIncrementalBuild() {
        return false;
    }

    /**
     * Incrementally updates existing index data with new values.
     *
     * <p>Implementations that return {@code true} from {@link #supportsIncrementalBuild()}
     * must override this method. The default throws {@link UnsupportedOperationException}.</p>
     *
     * @param existingData the current index data to update
     * @param newValues    iterator over new rows to add
     * @param properties   index configuration properties
     * @return updated index data incorporating the new values
     * @throws UnsupportedOperationException if incremental build is not supported
     */
    default IndexData buildIncremental(IndexData existingData,
                                       Iterator<List<Comparable<?>>> newValues,
                                       Map<String, String> properties) {
        throw new UnsupportedOperationException(
                supportedType() + " does not support incremental build");
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
