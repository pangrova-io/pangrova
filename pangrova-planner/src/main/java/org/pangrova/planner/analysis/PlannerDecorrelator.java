package org.pangrova.planner.analysis;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Safe decorrelation support: applies Calcite's RelDecorrelator with a
 * fallback to the original plan on failure, and preserves LATERAL scalar
 * aggregate semantics by rewriting post-decorrelation INNER joins to LEFT.
 */
final class PlannerDecorrelator {

    private static final Logger LOG = LoggerFactory.getLogger(PlannerDecorrelator.class);

    private PlannerDecorrelator() {
    }

    @SuppressWarnings("java:S1181")
    static RelNode safeDecorrelate(RelNode rel) {
        if (!containsLogicalCorrelate(rel)) {
            return rel;
        }
        // Track INNER correlates with scalar aggregate before decorrelation.
        // After decorrelation they become INNER JOINs which incorrectly drop rows.
        boolean hasInnerScalarCorrelate = containsInnerScalarCorrelate(rel);
        try {
            RelNode decorrelated = RelDecorrelator.decorrelateQuery(rel);
            LOG.debug("event=decorrelation_applied originalDigest={} newDigest={}",
                    rel.getDigest(), decorrelated.getDigest());
            // Post-process: if we had INNER correlate + scalar aggregate, the
            // decorrelator produces INNER JOIN + GROUP BY aggregate. Convert to
            // LEFT JOIN to preserve all left rows (matching original LATERAL
            // semantics where scalar aggregate always returns 1 row).
            if (hasInnerScalarCorrelate) {
                decorrelated = rewriteInnerJoinWithAggToLeft(decorrelated);
            }
            return decorrelated;
        } catch (Throwable t) {
            LOG.debug("event=decorrelation_skipped reason={}", t.getMessage());
            return rel;
        }
    }

    private static boolean containsInnerScalarCorrelate(RelNode node) {
        if (node instanceof LogicalCorrelate correlate
                && correlate.getJoinType() == org.apache.calcite.rel.core.JoinRelType.INNER
                && isScalarAggregateRight(correlate.getRight())) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsInnerScalarCorrelate(input)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Post-decorrelation rewrite: converts INNER JOINs whose right input is an
     * Aggregate (from scalar correlate decorrelation) to LEFT JOINs. This preserves
     * all left-side rows, matching the original LATERAL/scalar-aggregate semantics
     * where the subquery always produces exactly one output row per input row.
     */
    private static RelNode rewriteInnerJoinWithAggToLeft(RelNode node) {
        if (node instanceof org.apache.calcite.rel.logical.LogicalJoin join
                && join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.INNER
                && join.getRight() instanceof org.apache.calcite.rel.logical.LogicalAggregate) {
            node = join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    rewriteInnerJoinWithAggToLeft(join.getLeft()),
                    rewriteInnerJoinWithAggToLeft(join.getRight()),
                    org.apache.calcite.rel.core.JoinRelType.LEFT,
                    join.isSemiJoinDone());
        } else {
            var inputs = node.getInputs();
            if (!inputs.isEmpty()) {
                var newInputs = new java.util.ArrayList<RelNode>(inputs.size());
                boolean changed = false;
                for (RelNode input : inputs) {
                    RelNode rewritten = rewriteInnerJoinWithAggToLeft(input);
                    newInputs.add(rewritten);
                    if (rewritten != input) {
                        changed = true;
                    }
                }
                if (changed) {
                    node = node.copy(node.getTraitSet(), newInputs);
                }
            }
        }
        return node;
    }

    private static boolean isScalarAggregateRight(RelNode right) {
        // Strip optional Project wrapper
        if (right instanceof org.apache.calcite.rel.logical.LogicalProject proj) {
            right = proj.getInput();
        }
        return right instanceof org.apache.calcite.rel.logical.LogicalAggregate agg
                && agg.getGroupCount() == 0;
    }

    private static boolean containsLogicalCorrelate(RelNode node) {
        if (node instanceof LogicalCorrelate) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsLogicalCorrelate(input)) {
                return true;
            }
        }
        return false;
    }
}
