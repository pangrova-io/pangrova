package org.pangrova.indexing.api.eval;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.connector.spi.index.IndexEvalResult;
import org.pangrova.connector.spi.index.predicate.IndexPredicate;

/**
 * SPI contract for evaluating index predicates against a single data file.
 *
 * <p>This interface decouples index evaluation from the indexing module,
 * allowing both coordinator and worker to perform index-based data skipping
 * through the same contract. The coordinator uses it during split filtering;
 * a future worker-side implementation can use it for secondary pruning.</p>
 *
 * <p>Implementations are expected to handle index data loading (from cache
 * or storage), deserialization, and engine dispatch internally.</p>
 */
@Experimental
public interface IndexFileEvaluator {

    /**
     * Evaluates a single data file against an index and predicate.
     *
     * @param descriptor    index metadata (type, columns, generation)
     * @param dataFilePath  path to the data file being evaluated
     * @param indexFilePath path to the index file for this data file
     * @param predicate     the predicate to evaluate
     * @param fsAccessor    file system accessor for reading index data
     * @return SKIP if no rows can match, KEEP if rows definitely match,
     *         UNKNOWN if evaluation is inconclusive
     */
    IndexEvalResult evaluateFile(
            IndexDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor);

    /**
     * Evaluates a single data file given only the minimum metadata available
     * to a worker (no catalog/schema/table or column lists).
     *
     * <p>Implementations must NOT synthesize placeholder values for the
     * missing fields — those placeholders previously polluted logs and metrics.
     * Instead, route directly through whatever code path uses only the eval
     * descriptor's three fields.</p>
     *
     * <p>The default implementation builds a minimal {@link IndexDescriptor}
     * with empty (non-placeholder) catalog/schema/table values; concrete
     * implementations should override for full fidelity.</p>
     */
    default IndexEvalResult evaluateFile(
            IndexEvalDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor) {
        throw new UnsupportedOperationException(
                "Implementation must override evaluateFile(IndexEvalDescriptor, ...)");
    }
}
