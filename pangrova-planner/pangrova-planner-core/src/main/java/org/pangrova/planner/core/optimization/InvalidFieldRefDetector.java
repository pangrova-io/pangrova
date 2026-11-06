package org.pangrova.planner.core.optimization;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexShuttle;

/**
 * Detects invalid (negative-index) field references in a plan tree.
 * These are produced by a known Calcite REDUCE_EXPRESSIONS bug when
 * SUBSTRING/LIKE appears inside CASE/aggregate expressions.
 */
public final class InvalidFieldRefDetector {

    private InvalidFieldRefDetector() {}

    /**
     * Returns true if the plan tree contains any RexInputRef with negative index
     * or AggregateCall with negative argument index.
     */
    public static boolean containsInvalidFieldRef(RelNode root) {
        boolean[] found = {false};
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found[0]) {
                    return;
                }
                if (hasNegativeAggIndex(node)) {
                    found[0] = true;
                    return;
                }
                if (hasNegativeInputRef(node)) {
                    found[0] = true;
                    return;
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return found[0];
    }

    private static boolean hasNegativeAggIndex(RelNode node) {
        if (!(node instanceof org.apache.calcite.rel.core.Aggregate agg)) {
            return false;
        }
        for (org.apache.calcite.rel.core.AggregateCall call : agg.getAggCallList()) {
            for (int idx : call.getArgList()) {
                if (idx < 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasNegativeInputRef(RelNode node) {
        boolean[] found = {false};
        node.accept(new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitInputRef(RexInputRef inputRef) {
                if (inputRef.getIndex() < 0) {
                    found[0] = true;
                }
                return inputRef;
            }
        });
        return found[0];
    }
}
