package org.pangrova.planner.core.retrieval;

import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.pangrova.common.api.annotation.Internal;

/** Proves the supported typed single-source scope; printed Rex strings carry no authority. */
@Internal
public final class FilteredRetrievalEligibility {
    private static final Set<SqlKind> FILTER_KINDS = Set.of(
            SqlKind.AND, SqlKind.OR, SqlKind.NOT, SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.IS_NULL, SqlKind.IS_NOT_NULL,
            SqlKind.IS_TRUE, SqlKind.IS_FALSE, SqlKind.CAST, SqlKind.CASE);

    private FilteredRetrievalEligibility() {}

    /** Requires projects above filters, so payload/score evaluation can move after hydration. */
    public static boolean supported(RelNode input) {
        RelNode current = input;
        while (current instanceof LogicalProject project) {
            if (project.getProjects().stream().anyMatch(expression -> !safeProjection(expression))) {
                return false;
            }
            current = project.getInput();
        }
        boolean filtered = false;
        while (current instanceof LogicalFilter filter) {
            if (!safeFilter(filter.getCondition())) {
                return false;
            }
            filtered = true;
            current = filter.getInput();
        }
        return filtered && current instanceof LogicalTableScan
                && PhysicalRetrievalIdentityRewriter.supportedInput(current);
    }

    private static boolean safeFilter(RexNode expression) {
        if (expression instanceof RexInputRef || expression instanceof RexLiteral) {
            return true;
        }
        return expression instanceof RexCall call && !(call instanceof RexSubQuery)
                && call.getOperator().isDeterministic() && FILTER_KINDS.contains(call.getKind())
                && call.getOperands().stream().allMatch(FilteredRetrievalEligibility::safeFilter);
    }

    private static boolean safeProjection(RexNode expression) {
        if (safeFilter(expression)) {
            return true;
        }
        if (!(expression instanceof RexCall call) || call instanceof RexSubQuery
                || !call.getOperator().isDeterministic()) {
            return false;
        }
        String name = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
        return (name.equals("L2_DISTANCE") || call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR
                || call.getKind() == SqlKind.CAST)
                && call.getOperands().stream().allMatch(FilteredRetrievalEligibility::safeProjection);
    }
}
