package org.pangrova.planner.vector;

import org.pangrova.protocol.v2.CapabilityRef;
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
 * @param indexId              matching durable VECTOR artifact ID
 * @param retrievalCapability  exact canonical Native retrieval capability bound to the artifact
 * @param searchBudget         bounded algorithm-specific exploration budget
 * @param serializedPreFilters serialized WHERE-clause filter expressions (immutable)
 * @param hybridStrategy       hybrid search strategy (ANN_THEN_FILTER or FILTER_THEN_BRUTEFORCE)
 * @param indexGeneration      build generation of the matched index; -1 if unknown
 * @param sourceSnapshot       query source snapshot; empty when unavailable
 * @param indexSnapshot        indexed source snapshot/generation; empty when unavailable
 * @param indexFreshnessEpochMs index metadata freshness timestamp; zero when unavailable
 */
public record VectorSearchNode(
        String tableName,
        String vectorColumn,
        float[] queryVector,
        int topK,
        DistanceMetric metric,
        String indexId,
        CapabilityRef retrievalCapability,
        int searchBudget,
        List<String> serializedPreFilters,
        VectorHybridSearchStrategy hybridStrategy,
        int indexGeneration,
        String sourceSnapshot,
        String indexSnapshot,
        long indexFreshnessEpochMs) {

    public VectorSearchNode {
        queryVector = queryVector.clone();
        if (retrievalCapability == null
                || retrievalCapability.getCapabilityId().isBlank()
                || retrievalCapability.getSemanticVersion().isBlank()
                || retrievalCapability.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "retrievalCapability must be a complete canonical reference");
        }
        if (searchBudget <= 0 || searchBudget > 100_000) {
            throw new IllegalArgumentException("searchBudget is outside the bounded range");
        }
        serializedPreFilters = serializedPreFilters == null
                ? List.of()
                : List.copyOf(serializedPreFilters);
        sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
        indexSnapshot = indexSnapshot == null ? "" : indexSnapshot;
        if (indexFreshnessEpochMs < 0) {
            throw new IllegalArgumentException("indexFreshnessEpochMs must be non-negative");
        }
    }

    @Override
    public float[] queryVector() {
        return queryVector.clone();
    }

    /** Returns a copy with query/index consistency metadata bound by the Coordinator. */
    public VectorSearchNode withSearchMetadata(
            String newSourceSnapshot, String newIndexSnapshot, long freshnessEpochMs) {
        return new VectorSearchNode(
                tableName, vectorColumn, queryVector, topK, metric, indexId,
                retrievalCapability, searchBudget, serializedPreFilters,
                hybridStrategy, indexGeneration,
                newSourceSnapshot, newIndexSnapshot, freshnessEpochMs);
    }
}
