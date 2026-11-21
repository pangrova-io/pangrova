package org.pangrova.planner.analysis;

import org.apache.calcite.rel.RelNode;

/**
 * Static plan-shape predicates used by the coordinator to detect special
 * plan forms (constant folds, LIMIT 0, MERGE, UNNEST) before dispatch.
 */
final class PlanShapeChecks {

    private PlanShapeChecks() {
    }

    /**
     * Returns true if the given RelNode is a folded constant — a LogicalValues
     * with no inputs, produced by the constant folding step in plan().
     */
    static boolean isFoldedConstant(RelNode relNode) {
        return relNode instanceof org.apache.calcite.rel.logical.LogicalValues
                && relNode.getInputs().isEmpty();
    }

    /**
     * Checks whether the plan is a tableless constant expression (no TableScan,
     * TableModify, or Exchange anywhere in the tree). This covers both fully-folded
     * LogicalValues and expressions that the registered reduction rules retain
     * for the sole Native execution path.
     */
    static boolean isTablelessConstant(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableScan
                || node instanceof org.apache.calcite.rel.core.TableModify
                || node instanceof org.apache.calcite.rel.core.Exchange) {
            return false;
        }
        for (RelNode input : node.getInputs()) {
            if (!isTablelessConstant(input)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the plan is a LIMIT 0 sort (zero rows returned).
     * Used by the coordinator to skip worker dispatch for empty result sets.
     */
    static boolean isZeroFetchSort(RelNode relNode) {
        if (!(relNode instanceof org.apache.calcite.rel.core.Sort sort)) {
            return false;
        }
        if (sort.fetch == null) {
            return false;
        }
        if (sort.fetch instanceof org.apache.calcite.rex.RexLiteral literal) {
            Integer fetch = literal.getValueAs(Integer.class);
            return fetch != null && fetch == 0;
        }
        return false;
    }

    /**
     * Returns true if the plan is a MERGE DML operation.
     * Used by the coordinator to skip split-count checks for MERGE.
     */
    static boolean isMergeOperation(RelNode relNode) {
        return relNode instanceof org.apache.calcite.rel.core.TableModify modify
                && modify.getOperation()
                        == org.apache.calcite.rel.core.TableModify.Operation.MERGE;
    }

    /**
     * Returns true if the plan contains an UNNEST/UNCOLLECT node.
     * The native layer does not support UNNEST; the coordinator handles it locally.
     */
    static boolean containsUnnest(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Uncollect) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsUnnest(input)) {
                return true;
            }
        }
        return false;
    }
}
