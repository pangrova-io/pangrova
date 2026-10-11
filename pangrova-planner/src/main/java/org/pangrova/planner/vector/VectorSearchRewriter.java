package org.pangrova.planner.vector;

import org.pangrova.planner.PlanTable;

import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.RecallEstimationHint;
import org.pangrova.spi.index.vector.VectorHybridSearchStrategy;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Detects ANN query patterns in Calcite logical plans and rewrites them
 * into {@link VectorSearchNode} instances.
 *
 * <p>Pattern: {@code ORDER BY distance_func(col, literal_vector) ASC LIMIT K}
 * where {@code distance_func} is one of L2_DISTANCE, COSINE_DISTANCE,
 * or INNER_PRODUCT.</p>
 *
 * <p>Calcite types ({@link LogicalSort}, {@link RexNode}) are used internally
 * but never leak into {@link VectorSearchNode}'s public fields.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class VectorSearchRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchRewriter.class);

    // COSINE_SIMILARITY is intentionally excluded: it returns higher values for
    // more similar vectors, so correct ANN usage is ORDER BY ... DESC LIMIT K.
    // Since this rewriter only matches ASC ordering, including COSINE_SIMILARITY
    // would produce incorrect results (returning the LEAST similar vectors).
    // Users should use COSINE_DISTANCE for ASC-based ANN queries.
    private static final Map<String, DistanceMetric> FUNCTION_TO_METRIC = Map.of(
            "L2_DISTANCE", DistanceMetric.L2,
            "COSINE_DISTANCE", DistanceMetric.COSINE,
            "INNER_PRODUCT", DistanceMetric.INNER_PRODUCT);

    private static final Set<String> DISTANCE_FUNCTIONS = FUNCTION_TO_METRIC.keySet();

    private static final RecallEstimationHint RECALL_ESTIMATOR = RecallEstimationHint.defaultEstimator();

    private VectorSearchRewriter() {}

    /**
     * Attempts to rewrite a {@link LogicalSort} into a {@link VectorSearchNode}.
     *
     * @param sortNode    the Calcite sort node (ORDER BY ... LIMIT K)
     * @param indexLookup function: (tableName, columnName) → matching VECTOR IndexDescriptors
     * @return the rewritten node, or empty if the pattern does not match
     */
    public static Optional<VectorSearchNode> tryRewrite(
            LogicalSort sortNode,
            BiFunction<String, String, List<IndexDescriptor>> indexLookup) {

        VectorCandidate candidate = parseCandidate(sortNode);
        if (candidate == null) {
            return Optional.empty();
        }

        String tableName = extractTableName(sortNode.getInput());
        IndexDescriptor matchingIndex = findMatchingIndex(
                indexLookup, tableName, candidate.call.column, candidate.call.metric);

        if (matchingIndex == null) {
            // No VECTOR index available — let the query proceed through the
            // standard native Substrait execution path (ORDER BY + LIMIT).
            // Native vectorized execution is faster than any Java-side
            // brute-force scan for unindexed vector queries.
            return Optional.empty();
        }

        validateDistanceMetricCompatibility(matchingIndex, candidate.call.metric);

        List<String> preFilters = extractPreFilters(sortNode.getInput());
        VectorHybridSearchStrategy strategy;
        String algorithm = matchingIndex.properties().getOrDefault("algorithm", "HNSW");
        CapabilityRef retrievalCapability =
                BuiltInCapabilityCatalog.vectorRetrievalPlanRef(algorithm);
        if (!org.pangrova.planner.retrieval.PhysicalRetrievalIdentityRewriter
                .supportedInput(sortNode.getInput())) {
            return Optional.empty();
        }
        if (!preFilters.isEmpty()) {
            if (!BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(
                    retrievalCapability.getCapabilityId())
                    || !org.pangrova.planner.retrieval.FilteredRetrievalEligibility
                            .supported(sortNode.getInput())) {
                return Optional.empty();
            }
            retrievalCapability = BuiltInCapabilityCatalog.filteredDiskAnnRetrievalPlanRef();
        }
        int dimension = Integer.parseInt(matchingIndex.properties().getOrDefault("dimension", "128"));
        double estimatedRecall = RECALL_ESTIMATOR.estimateRecall(
                candidate.topK, dimension, algorithm, 0);

        if (preFilters.isEmpty()) {
            int effectiveK = candidate.topK;
            if (estimatedRecall < 0.9) {
                effectiveK = (int) Math.ceil(candidate.topK * (1.0 / estimatedRecall));
            }
            strategy = new VectorHybridSearchStrategy(
                    VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, effectiveK);
        } else {
            int effectiveK = candidate.topK;
            strategy = new VectorHybridSearchStrategy(
                    VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, effectiveK);
        }

        return Optional.of(new VectorSearchNode(
                tableName, candidate.call.column, candidate.call.queryVector,
                candidate.topK, candidate.call.metric,
                matchingIndex.indexId(),
                retrievalCapability,
                searchBudget(matchingIndex, retrievalCapability, strategy.effectiveK()),
                preFilters,
                strategy,
                matchingIndex.generation(),
                "",
                "",
                0));
    }

    private static int searchBudget(
            IndexDescriptor descriptor, CapabilityRef retrievalCapability, int effectiveK) {
        Map<String, String> properties = descriptor.properties();
        int dynamic = (int) Math.min(
                100_000L, Math.max(64L, Math.multiplyExact((long) effectiveK, 4L)));
        String capabilityId = retrievalCapability.getCapabilityId();
        if (BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)) {
            return boundedProperty(
                    properties, "ef_search", dynamic, effectiveK, 100_000);
        }
        if (BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)) {
            int clusters = boundedProperty(
                    properties, "coarse_clusters", 64, 1, 4_096);
            return boundedProperty(
                    properties, "nprobes", Math.min(8, clusters), 1, clusters);
        }
        if (BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(capabilityId)
                || BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID.equals(capabilityId)) {
            return boundedProperty(
                    properties, "search_list_size", dynamic, effectiveK, 100_000);
        }
        throw new IllegalArgumentException(
                "Unsupported Native vector retrieval capability: " + capabilityId);
    }

    private static int boundedProperty(
            Map<String, String> properties, String key,
            int defaultValue, int minimum, int maximum) {
        final int value;
        try {
            value = Integer.parseInt(properties.getOrDefault(key, Integer.toString(defaultValue)));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(key + " must be an integer", failure);
        }
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " is outside the bounded range");
        }
        return value;
    }

    /**
     * Validates sort-key, topK, sort expression, and distance call preconditions.
     * Returns null if any guard fails.
     */
    private static VectorCandidate parseCandidate(LogicalSort sortNode) {
        if (!hasSingleAscSortKey(sortNode)) {
            return null;
        }
        int topK = extractTopK(sortNode);
        if (topK < 1) {
            return null;
        }
        RexNode sortKey = sortNode.getCollation().getFieldCollations().get(0).getFieldIndex() >= 0
                ? extractSortExpression(sortNode)
                : null;
        if (sortKey == null) {
            return null;
        }
        DistanceCall call = parseDistanceCall(sortKey, sortNode.getInput());
        return call != null ? new VectorCandidate(topK, call) : null;
    }

    private record VectorCandidate(int topK, DistanceCall call) {}

    private static boolean hasSingleAscSortKey(LogicalSort sort) {
        if (sort.getCollation() == null) {
            return false;
        }
        var collations = sort.getCollation().getFieldCollations();
        if (collations.size() != 1) {
            return false;
        }
        return !collations.get(0).getDirection().isDescending();
    }

    private static int extractTopK(LogicalSort sort) {
        if (sort.fetch == null) {
            return -1;
        }
        if (sort.fetch instanceof RexLiteral literal) {
            Long val = literal.getValueAs(Long.class);
            return val != null && val > 0 && val <= Integer.MAX_VALUE ? val.intValue() : -1;
        }
        return -1;
    }

    /**
     * Extracts the sort expression from the input's project list using the
     * sort field index. The sort node references a field ordinal in its input;
     * if the input is a Project, we resolve the underlying expression.
     */
    private static RexNode extractSortExpression(LogicalSort sort) {
        int fieldIdx = sort.getCollation().getFieldCollations().get(0).getFieldIndex();
        RelNode input = sort.getInput();
        if (input instanceof org.apache.calcite.rel.logical.LogicalProject project) {
            if (fieldIdx < project.getProjects().size()) {
                return project.getProjects().get(fieldIdx);
            }
        }
        return null;
    }

    private static DistanceCall parseDistanceCall(RexNode expr, RelNode input) {
        if (!(expr instanceof RexCall call)
                || !DISTANCE_FUNCTIONS.contains(call.getOperator().getName().toUpperCase())
                || call.getOperands().size() != 2) {
            return null;
        }
        // Try canonical order: column, vector
        String columnName = extractColumnName(call.getOperands().get(0), input);
        float[] queryVector = extractVectorLiteral(call.getOperands().get(1));
        // Try reversed order: vector, column
        if (columnName == null || queryVector == null) {
            columnName = extractColumnName(call.getOperands().get(1), input);
            queryVector = extractVectorLiteral(call.getOperands().get(0));
        }
        if (columnName == null || queryVector == null) {
            return null;
        }
        return new DistanceCall(columnName, queryVector,
                FUNCTION_TO_METRIC.get(call.getOperator().getName().toUpperCase()));
    }

    /**
     * Resolves the actual column name from a {@link org.apache.calcite.rex.RexInputRef}.
     *
     * <p>{@code RexInputRef.getName()} returns synthetic names like {@code $0}, {@code $1}.
     * The real column name must be resolved from the input's row type using the field index.</p>
     */
    private static String extractColumnName(RexNode node, RelNode input) {
        if (node instanceof RexCall cast && cast.getKind() == org.apache.calcite.sql.SqlKind.CAST
                && samePhysicalArrayElement(cast.getType(), cast.getOperands().get(0).getType())) {
            return extractColumnName(cast.getOperands().get(0), input);
        }
        if (node instanceof org.apache.calcite.rex.RexInputRef ref) {
            int fieldIndex = ref.getIndex();
            // Resolve from the underlying input's row type (e.g., LogicalProject or TableScan)
            RelNode resolveFrom = input;
            if (resolveFrom instanceof org.apache.calcite.rel.logical.LogicalProject project) {
                // The RexInputRef inside the distance function refers to the Project's input
                resolveFrom = project.getInput();
            }
            var rowType = resolveFrom.getRowType();
            if (fieldIndex >= 0 && fieldIndex < rowType.getFieldCount()) {
                return rowType.getFieldList().get(fieldIndex).getName();
            }
        }
        return null;
    }

    private static float[] extractVectorLiteral(RexNode node) {
        if (node instanceof RexCall cast && cast.getKind() == org.apache.calcite.sql.SqlKind.CAST
                && floatingArray(cast.getType())) {
            return extractVectorLiteral(cast.getOperands().get(0));
        }
        if (node instanceof RexCall call && "ARRAY".equalsIgnoreCase(call.getOperator().getName())) {
            return extractFromArrayCall(call);
        }
        if (node instanceof RexLiteral literal) {
            return extractFromListLiteral(literal);
        }
        return null;
    }

    private static boolean samePhysicalArrayElement(org.apache.calcite.rel.type.RelDataType left,
            org.apache.calcite.rel.type.RelDataType right) {
        var leftElement = left.getComponentType();
        var rightElement = right.getComponentType();
        if (!floatingArray(left) || !floatingArray(right) || leftElement == null || rightElement == null) return false;
        return (leftElement.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.DOUBLE)
                == (rightElement.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    }

    private static boolean floatingArray(org.apache.calcite.rel.type.RelDataType type) {
        if (type.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.ARRAY) return false;
        var component = type.getComponentType();
        if (component == null) return false;
        return switch (component.getSqlTypeName()) {
            case FLOAT, REAL, DOUBLE -> true;
            default -> false;
        };
    }

    private static float[] extractFromArrayCall(RexCall call) {
        float[] result = new float[call.getOperands().size()];
        for (int i = 0; i < result.length; i++) {
            RexNode element = call.getOperands().get(i);
            if (!(element instanceof RexLiteral literal)) {
                return null;
            }
            Number val = literal.getValueAs(Number.class);
            if (val == null) {
                return null;
            }
            result[i] = val.floatValue();
        }
        return result;
    }

    private static float[] extractFromListLiteral(RexLiteral literal) {
        List<?> list = literal.getValueAs(List.class);
        if (list == null) {
            return null;
        }
        float[] result = new float[list.size()];
        for (int i = 0; i < result.length; i++) {
            if (!(list.get(i) instanceof Number num)) {
                return null;
            }
            result[i] = num.floatValue();
        }
        return result;
    }

    private static String extractTableName(RelNode input) {
        RelNode current = input;
        while (current != null) {
            if (current instanceof org.apache.calcite.rel.logical.LogicalTableScan scan) {
                return String.join(".", PlanTable.require(scan)
                        .getQualifiedName());
            }
            if (current.getInputs().isEmpty()) {
                break;
            }
            current = current.getInput(0);
        }
        return "unknown";
    }

    private static List<String> extractPreFilters(RelNode input) {
        var filters = new java.util.ArrayList<String>();
        RelNode current = input;
        while (current != null) {
            if (current instanceof LogicalFilter filter) {
                List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
                conjuncts.stream().map(RexNode::toString).forEach(filters::add);
            }
            if (current.getInputs().isEmpty()) {
                break;
            }
            if (current.getInputs().size() != 1) {
                break;
            }
            current = current.getInput(0);
        }
        return List.copyOf(filters);
    }

    private static IndexDescriptor findMatchingIndex(
            BiFunction<String, String, List<IndexDescriptor>> indexLookup,
            String tableName, String column, DistanceMetric metric) {
        List<IndexDescriptor> descriptors = indexLookup.apply(tableName, column);
        if (descriptors == null) {
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

    /**
     * Validates that the query distance metric is compatible with the index's
     * configured metric. Incompatible combinations (e.g., L2 query on COSINE index)
     * produce incorrect nearest-neighbor results.
     *
     * @throws IllegalArgumentException if the metric is incompatible
     */
    private static void validateDistanceMetricCompatibility(
            IndexDescriptor index, DistanceMetric queryMetric) {
        String indexMetricStr = index.properties().get("metric");
        if (indexMetricStr == null) {
            return; // no metric configured — skip validation
        }
        DistanceMetric indexMetric;
        try {
            indexMetric = DistanceMetric.valueOf(indexMetricStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("event=unknown_index_metric indexId={} metric={} action=skip_validation",
                    index.indexId(), indexMetricStr);
            return;
        }
        // COSINE_SIMILARITY and COSINE_DISTANCE both use cosine-based indexes
        if (queryMetric == indexMetric) {
            return;
        }
        // L2 index is only valid for L2 queries; COSINE index for COSINE queries;
        // INNER_PRODUCT index for INNER_PRODUCT queries
        throw new IllegalArgumentException(
                "Distance metric mismatch: query uses " + queryMetric
                        + " but index '" + index.indexId() + "' is built with " + indexMetric
                        + ". Rebuild the index with metric=" + queryMetric
                        + " or change the query distance function.");
    }

    private record DistanceCall(String column, float[] queryVector, DistanceMetric metric) {}
}
