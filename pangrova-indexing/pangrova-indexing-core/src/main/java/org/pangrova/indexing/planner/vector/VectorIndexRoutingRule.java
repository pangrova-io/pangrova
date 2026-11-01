package org.pangrova.indexing.planner.vector;

import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.vector.DistanceMetric;
import org.pangrova.spi.indexing.vector.VectorHybridSearchStrategy;
import org.pangrova.spi.indexing.vector.VectorHybridSearchStrategy.Mode;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Decides between ANN index scan and full scan for a recognized VectorTopK query.
 *
 * <p>Routing logic:
 * <ol>
 *   <li>If {@code NO_VECTOR_INDEX} hint is active → full scan (exact search)</li>
 *   <li>If a READY vector index exists with matching metric → {@link VectorSearchNode} (ANN)</li>
 *   <li>If no matching index → full scan fallback (FullScanNode + distance + TopK sort)</li>
 * </ol>
 *
 * <p>Pre-filter vs post-filter decision: when estimated selectivity is below
 * {@link #PRE_FILTER_SELECTIVITY_THRESHOLD} (10%), the filter runs before the
 * vector index scan (FILTER_THEN_BRUTEFORCE). Otherwise, ANN runs first with
 * over-fetched K, then post-filters.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class VectorIndexRoutingRule {

    private static final Logger LOG = LoggerFactory.getLogger(VectorIndexRoutingRule.class);

    /**
     * Selectivity threshold below which pre-filtering is applied.
     * When less than 10% of rows survive the WHERE clause, filter first
     * then brute-force scan the survivors.
     */
    static final double PRE_FILTER_SELECTIVITY_THRESHOLD = 0.10;

    /** Over-fetch multiplier for ANN_THEN_FILTER mode. */
    private static final double OVER_FETCH_MULTIPLIER = 1.5;

    /** Upper bound for algorithm-specific exploration budgets (search_list_size/ef_search). */
    private static final int MAX_SEARCH_BUDGET = 100_000;

    /** SQL hint name that forces exact search, bypassing vector index. */
    public static final String NO_VECTOR_INDEX_HINT = "NO_VECTOR_INDEX";

    private VectorIndexRoutingRule() {}

    /**
     * Routes a VectorTopK candidate to either an ANN index scan or full scan.
     *
     * @param candidate       recognized VectorTopK pattern
     * @param indexLookup     function: (tableName, columnName) → available IndexDescriptors
     * @param forceExactSearch true if {@code NO_VECTOR_INDEX} hint is present
     * @param estimatedSelectivity estimated fraction of rows surviving WHERE predicates (0.0 to 1.0);
     *                             1.0 means no WHERE filter (all rows survive)
     * @param totalRows       total row count in the table (for over-fetch calculation)
     * @param serializedPreFilters serialized WHERE-clause filter expressions
     * @return routing decision: present = ANN index scan, empty = full scan fallback
     */
    public static Optional<VectorSearchNode> route(
            VectorTopKCandidate candidate,
            BiFunction<String, String, List<IndexDescriptor>> indexLookup,
            boolean forceExactSearch,
            double estimatedSelectivity,
            long totalRows,
            List<String> serializedPreFilters) {

        if (forceExactSearch) {
            LOG.debug("event=vector_index_bypass reason=no_vector_index_hint table={} column={}",
                    candidate.tableName(), candidate.vectorColumn());
            return Optional.empty();
        }

        IndexDescriptor matchingIndex = findMatchingIndex(
                indexLookup, candidate.tableName(), candidate.vectorColumn(), candidate.metric());

        if (matchingIndex == null) {
            LOG.debug("event=vector_full_scan reason=no_matching_index table={} column={} metric={}",
                    candidate.tableName(), candidate.vectorColumn(), candidate.metric());
            return Optional.empty();
        }

        validateMetricCompatibility(matchingIndex, candidate.metric());

        VectorHybridSearchStrategy strategy = selectStrategy(
                estimatedSelectivity, candidate.topK(), totalRows, serializedPreFilters);

        return Optional.of(new VectorSearchNode(
                candidate.tableName(),
                candidate.vectorColumn(),
                candidate.queryVector(),
                candidate.topK(),
                candidate.metric(),
                matchingIndex.indexId(),
                retrievalCapability(matchingIndex),
                searchBudget(matchingIndex, strategy.effectiveK()),
                serializedPreFilters,
                strategy,
                matchingIndex.generation(),
                "",
                "",
                0));
    }

    private static CapabilityRef retrievalCapability(IndexDescriptor descriptor) {
        String value = descriptor.properties().getOrDefault("algorithm", "HNSW")
                .trim().toUpperCase().replace('-', '_');
        return BuiltInCapabilityCatalog.vectorRetrievalPlanRef(value);
    }

    private static int searchBudget(IndexDescriptor descriptor, int effectiveK) {
        CapabilityRef capability = retrievalCapability(descriptor);
        String capabilityId = capability.getCapabilityId();
        int dynamic = (int) Math.min(
                (long) MAX_SEARCH_BUDGET, Math.max(64L, Math.multiplyExact((long) effectiveK, 4L)));
        String key;
        int defaultValue;
        int maximum;
        if (BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)) {
            key = "nprobes";
            maximum = Integer.parseInt(
                    descriptor.properties().getOrDefault("coarse_clusters", "64"));
            defaultValue = Math.min(8, maximum);
        } else {
            key = BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)
                    ? "ef_search"
                    : "search_list_size";
            maximum = MAX_SEARCH_BUDGET;
            defaultValue = dynamic;
        }
        int value = Integer.parseInt(
                descriptor.properties().getOrDefault(key, Integer.toString(defaultValue)));
        int minimum = BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)
                ? 1
                : effectiveK;
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " is outside the bounded range");
        }
        return value;
    }

    /**
     * Simplified routing without pre-filters (no WHERE clause).
     *
     * @param candidate       recognized VectorTopK pattern
     * @param indexLookup     function: (tableName, columnName) → available IndexDescriptors
     * @param forceExactSearch true if {@code NO_VECTOR_INDEX} hint is present
     * @return routing decision: present = ANN index scan, empty = full scan fallback
     */
    public static Optional<VectorSearchNode> route(
            VectorTopKCandidate candidate,
            BiFunction<String, String, List<IndexDescriptor>> indexLookup,
            boolean forceExactSearch) {
        return route(candidate, indexLookup, forceExactSearch, 1.0, Long.MAX_VALUE, List.of());
    }

    /**
     * Checks whether the given SQL text contains the {@code NO_VECTOR_INDEX} hint.
     *
     * <p>Detects the pattern {@code /*+ NO_VECTOR_INDEX * /} in the SQL string.
     * Since the ANTLR lexer strips block comments before parsing, hint detection
     * must occur on the raw SQL text before parse.</p>
     *
     * @param sql raw SQL text
     * @return true if the hint is present
     */
    public static boolean containsNoVectorIndexHint(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }
        int hintStart = sql.indexOf("/*+");
        if (hintStart < 0) {
            return false;
        }
        int hintEnd = sql.indexOf("*/", hintStart);
        if (hintEnd < 0) {
            return false;
        }
        String hintBody = sql.substring(hintStart + 3, hintEnd).trim().toUpperCase();
        return hintBody.contains(NO_VECTOR_INDEX_HINT);
    }

    private static VectorHybridSearchStrategy selectStrategy(
            double estimatedSelectivity, int topK, long totalRows,
            List<String> preFilters) {

        boolean hasPreFilters = preFilters != null && !preFilters.isEmpty();

        if (!hasPreFilters) {
            return new VectorHybridSearchStrategy(Mode.ANN_THEN_FILTER, topK);
        }

        if (estimatedSelectivity < PRE_FILTER_SELECTIVITY_THRESHOLD) {
            return new VectorHybridSearchStrategy(Mode.FILTER_THEN_BRUTEFORCE, topK);
        }

        long overFetchK = Math.min(
                (long) Math.ceil(topK / estimatedSelectivity * OVER_FETCH_MULTIPLIER),
                totalRows);
        int effectiveK = (int) Math.max(topK, Math.min(overFetchK, Integer.MAX_VALUE));
        return new VectorHybridSearchStrategy(Mode.ANN_THEN_FILTER, effectiveK);
    }

    private static IndexDescriptor findMatchingIndex(
            BiFunction<String, String, List<IndexDescriptor>> indexLookup,
            String tableName, String column, DistanceMetric metric) {
        List<IndexDescriptor> descriptors = indexLookup.apply(tableName, column);
        if (descriptors == null || descriptors.isEmpty()) {
            return null;
        }
        return descriptors.stream()
                .filter(d -> d.indexType() == IndexType.VECTOR)
                .filter(d -> d.state() == IndexState.READY)
                .filter(d -> metricMatches(d, metric))
                .findFirst()
                .orElse(null);
    }

    private static boolean metricMatches(IndexDescriptor descriptor, DistanceMetric metric) {
        String indexMetric = descriptor.properties().get("metric");
        return indexMetric != null && indexMetric.equalsIgnoreCase(metric.name());
    }

    private static void validateMetricCompatibility(IndexDescriptor index, DistanceMetric queryMetric) {
        String indexMetricStr = index.properties().get("metric");
        if (indexMetricStr == null) {
            return;
        }
        DistanceMetric indexMetric;
        try {
            indexMetric = DistanceMetric.valueOf(indexMetricStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("event=unknown_index_metric indexId={} metric={} action=skip_validation",
                    index.indexId(), indexMetricStr);
            return;
        }
        if (queryMetric != indexMetric) {
            throw new IllegalArgumentException(
                    "Distance metric mismatch: query uses " + queryMetric
                            + " but index '" + index.indexId() + "' is built with " + indexMetric
                            + ". Rebuild the index with metric=" + queryMetric
                            + " or change the query distance function.");
        }
    }
}
