package org.pangrova.spi.connector.index;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.predicate.IndexPredicate;

import java.util.List;

/**
 * SPI contract for evaluating native (format-level) indexes against predicates.
 *
 * <p>Connectors implement this interface to expose their native index
 * evaluation capabilities (e.g. Parquet min/max statistics, ORC bloom filters)
 * to the Pangrova indexing pipeline as L1 (native) evaluation.
 */
@Experimental
public interface NativeIndexEvaluator {

    /**
     * Evaluates native indexes for the specified data file against the given
     * predicates, considering the declared capabilities.
     *
     * <p>The evaluation contract guarantees no false negatives: if the data
     * file contains matching rows, the result is never {@link IndexEvalResult#SKIP}.
     *
     * @param dataFilePath absolute path to the data file
     * @param predicates   predicates to evaluate
     * @param capabilities native index capabilities declared by the connector
     * @return the evaluation result
     */
    IndexEvalResult evaluate(
            String dataFilePath,
            List<IndexPredicate> predicates,
            List<NativeIndexCapability> capabilities);
}
