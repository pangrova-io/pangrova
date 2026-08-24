package org.pangrova.planner.vector;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorHybridSearchStrategy;

import java.util.List;

/**
 * Logical plan node representing an ANN (Approximate Nearest Neighbor) search.
 *
 * <p>Produced by {@link VectorSearchRewriter} when it detects the pattern
 * {@code ORDER BY distance_func(col, literal_vector) ASC LIMIT K}.
 * Consumed by the Coordinator's {@code QueryDispatcher} for vector task dispatch.</p>
 *
 * <p>All fields use SPI types or primitives — no Calcite types leak here.
 * Pre-filter predicates are serialized as strings, consistent with the
 * existing predicate push-down protocol in {@code QueryDispatcher}.</p>
 *
 * @param tableName            fully qualified table name
 * @param vectorColumn         name of the vector column
 * @param queryVector          query embedding (defensively copied)
 * @param topK                 number of nearest neighbors to return
 * @param metric               distance metric for similarity computation
 * @param indexId              matching VECTOR index ID; {@code null} triggers brute-force
 * @param serializedPreFilters serialized WHERE-clause filter expressions (immutable)
 * @param hybridStrategy       hybrid search strategy (ANN_THEN_FILTER or FILTER_THEN_BRUTEFORCE)
 * @param indexGeneration      build generation of the matched index; -1 if unknown
 */
public record VectorSearchNode(
        String tableName,
        String vectorColumn,
        float[] queryVector,
        int topK,
        DistanceMetric metric,
        String indexId,
        List<String> serializedPreFilters,
        VectorHybridSearchStrategy hybridStrategy,
        int indexGeneration) {

    public VectorSearchNode {
        queryVector = queryVector.clone();
        serializedPreFilters = serializedPreFilters == null
                ? List.of()
                : List.copyOf(serializedPreFilters);
    }
}
