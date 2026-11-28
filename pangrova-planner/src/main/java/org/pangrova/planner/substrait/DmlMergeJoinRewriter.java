package org.pangrova.planner.substrait;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/** Builds a target-preserving MERGE join with a non-null source-match marker. */
final class DmlMergeJoinRewriter {
    private DmlMergeJoinRewriter() {}

    static LogicalJoin swapToTargetFirstLeftJoin(LogicalJoin join, RexBuilder rexBuilder) {
        int sourceFieldCount = join.getLeft().getRowType().getFieldCount();
        int targetFieldCount = join.getRight().getRowType().getFieldCount();
        // Remap condition from (source|target) to (target|source) layout.
        org.apache.calcite.rex.RexShuttle swapShuttle =
                new org.apache.calcite.rex.RexShuttle() {
                    @Override
                    public RexNode visitInputRef(
                            org.apache.calcite.rex.RexInputRef inputRef) {
                        int idx = inputRef.getIndex();
                        int newIdx = (idx < sourceFieldCount)
                                ? targetFieldCount + idx
                                : idx - sourceFieldCount;
                        return rexBuilder.makeInputRef(inputRef.getType(), newIdx);
                    }
                };
        RexNode remappedCondition = join.getCondition().accept(swapShuttle);
        RelNode markedSource = sourceWithMatchMarker(join.getLeft(), rexBuilder);
        return join.copy(
                join.getTraitSet(), remappedCondition,
                join.getRight(), markedSource,
                JoinRelType.LEFT, join.isSemiJoinDone());
    }

    private static RelNode sourceWithMatchMarker(RelNode source, RexBuilder rexBuilder) {
        List<RexNode> expressions = new java.util.ArrayList<>();
        var fields = source.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
            expressions.add(rexBuilder.makeInputRef(fields.get(i).getType(), i));
        }
        expressions.add(rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE));
        List<String> names = new java.util.ArrayList<>(source.getRowType().getFieldNames());
        names.add("__merge_match_marker");
        var rowType = rexBuilder.getTypeFactory().createStructType(
                expressions.stream().map(RexNode::getType).toList(), names);
        return LogicalProject.create(source, List.of(), expressions, rowType);
    }
}
