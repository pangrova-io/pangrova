package org.pangrova.planner.vector;

import org.pangrova.planner.PlanTable;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Detects the VectorTopK query pattern in Calcite logical plans.
 *
 * <p>Pattern: {@code ORDER BY {cosine_distance|l2_distance|inner_product}(vec_col, query) LIMIT K}
 *
 * <p>This class is responsible only for pattern recognition. Routing decisions
 * (index vs full scan, pre/post filter) are handled by {@link VectorIndexRoutingRule}.
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class VectorTopKRecognizer {

    private static final Map<String, DistanceMetric> FUNCTION_TO_METRIC = Map.of(
            "L2_DISTANCE", DistanceMetric.L2,
            "COSINE_DISTANCE", DistanceMetric.COSINE,
            "INNER_PRODUCT", DistanceMetric.INNER_PRODUCT);

    private static final Set<String> DISTANCE_FUNCTIONS = FUNCTION_TO_METRIC.keySet();

    private VectorTopKRecognizer() {}

    /**
     * Attempts to recognize a VectorTopK pattern from a LogicalSort node.
     *
     * @param sortNode the Calcite sort node (ORDER BY ... LIMIT K)
     * @return recognized candidate if the pattern matches, empty otherwise
     */
    public static Optional<VectorTopKCandidate> recognize(LogicalSort sortNode) {
        if (!hasSingleAscSortKey(sortNode)) {
            return Optional.empty();
        }
        int topK = extractTopK(sortNode);
        if (topK < 1) {
            return Optional.empty();
        }
        RexNode sortExpr = extractSortExpression(sortNode);
        if (sortExpr == null) {
            return Optional.empty();
        }
        DistanceCallInfo callInfo = parseDistanceCall(sortExpr, sortNode.getInput());
        if (callInfo == null) {
            return Optional.empty();
        }
        String tableName = extractTableName(sortNode.getInput());
        return Optional.of(new VectorTopKCandidate(
                tableName, callInfo.column(), callInfo.queryVector(),
                topK, callInfo.metric()));
    }

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

    private static RexNode extractSortExpression(LogicalSort sort) {
        int fieldIdx = sort.getCollation().getFieldCollations().get(0).getFieldIndex();
        RelNode input = sort.getInput();
        if (input instanceof LogicalProject project) {
            if (fieldIdx < project.getProjects().size()) {
                return project.getProjects().get(fieldIdx);
            }
        }
        return null;
    }

    private static DistanceCallInfo parseDistanceCall(RexNode expr, RelNode input) {
        if (!(expr instanceof RexCall call)
                || !DISTANCE_FUNCTIONS.contains(call.getOperator().getName().toUpperCase())
                || call.getOperands().size() != 2) {
            return null;
        }
        String columnName = extractColumnName(call.getOperands().get(0), input);
        float[] queryVector = extractVectorLiteral(call.getOperands().get(1));
        if (columnName == null || queryVector == null) {
            columnName = extractColumnName(call.getOperands().get(1), input);
            queryVector = extractVectorLiteral(call.getOperands().get(0));
        }
        if (columnName == null || queryVector == null) {
            return null;
        }
        DistanceMetric metric = FUNCTION_TO_METRIC.get(call.getOperator().getName().toUpperCase());
        return new DistanceCallInfo(columnName, queryVector, metric);
    }

    private static String extractColumnName(RexNode node, RelNode input) {
        if (node instanceof RexInputRef ref) {
            int fieldIndex = ref.getIndex();
            RelNode resolveFrom = input;
            if (resolveFrom instanceof LogicalProject project) {
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
        if (node instanceof RexCall call && "ARRAY".equalsIgnoreCase(call.getOperator().getName())) {
            return extractFromArrayCall(call);
        }
        if (node instanceof RexLiteral literal) {
            return extractFromListLiteral(literal);
        }
        return null;
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
            if (current instanceof LogicalTableScan scan) {
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

    private record DistanceCallInfo(String column, float[] queryVector, DistanceMetric metric) {}
}
