package org.pangrova.planner.analysis;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.enums.DistributionType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Proves the narrow streaming map semantics rather than guessing from table names. */
final class PartitionLocalPlanInspector {

    private PartitionLocalPlanInspector() {}

    static boolean isIndependent(RelNode relation) {
        if (relation instanceof TableScan) {
            return true;
        }
        if (!(relation instanceof Filter) && !(relation instanceof Project)) {
            return false;
        }
        return localExpressions(relation) && relation.getInputs().size() == 1
                && isIndependent(relation.getInput(0));
    }

    /** Coalescing is safe when every grouping key covers its original hash partition keys. */
    static boolean supportsCoalescedHashInput(RelNode plan, String exchangeId, List<Integer> hashKeys) {
        RelNode input = plan instanceof PangrovaExchange exchange ? exchange.getInput() : plan;
        return !hashKeys.isEmpty() && preservedHashKeys(input, exchangeId, ImmutableBitSet.of(hashKeys)) != null;
    }

    /** Proves that every possible match is already assigned to the same original bucket on both inputs. */
    static Optional<PlanFragment.AlignedHashJoin> alignedHashJoin(
            RelNode plan, Map<String, List<Integer>> exchanges, int partitions) {
        if (exchanges.size() != 2) return Optional.empty();
        RelNode input = plan instanceof PangrovaExchange exchange ? exchange.getInput() : plan;
        while (input instanceof Project || input instanceof Filter) {
            if (!localExpressions(input)) return Optional.empty();
            input = input.getInput(0);
        }
        if (!(input instanceof Join join)
                || !(join.getLeft() instanceof PangrovaExchangeInput left)
                || !(join.getRight() instanceof PangrovaExchangeInput right)
                || left.exchangeId().equals(right.exchangeId())) {
            return Optional.empty();
        }
        var keys = join.analyzeCondition();
        if (!supportsHashPartitioning(join, keys)
                || !matchesJoinExchange(left, exchanges.get(left.exchangeId()), keys.leftKeys, partitions)
                || !matchesJoinExchange(right, exchanges.get(right.exchangeId()), keys.rightKeys, partitions)) {
            return Optional.empty();
        }
        var proof = switch (join.getJoinType()) {
            case INNER -> new PlanFragment.AlignedHashJoin(left.exchangeId(), right.exchangeId(), true, true);
            case LEFT, SEMI -> new PlanFragment.AlignedHashJoin(left.exchangeId(), right.exchangeId(), true, false);
            case RIGHT -> new PlanFragment.AlignedHashJoin(left.exchangeId(), right.exchangeId(), false, true);
            case FULL -> new PlanFragment.AlignedHashJoin(left.exchangeId(), right.exchangeId(), false, false);
            default -> throw new IllegalStateException("Unexpected aligned hash join type " + join.getJoinType());
        };
        return Optional.of(proof);
    }

    static boolean supportsHashPartitioning(Join join, JoinInfo keys) {
        return !keys.leftKeys.isEmpty() && partitionLocalJoin(join)
                && sameHashTypes(join.getLeft(), join.getRight(), keys.leftKeys, keys.rightKeys);
    }

    private static boolean partitionLocalJoin(Join join) {
        return switch (join.getJoinType()) {
            case INNER, LEFT, RIGHT, FULL, SEMI -> join.getVariablesSet().isEmpty() && localExpressions(join);
            default -> false;
        };
    }

    private static boolean matchesJoinExchange(PangrovaExchangeInput input, List<Integer> edgeKeys,
            List<Integer> joinKeys, int partitions) {
        return input.requireComplete() && input.partitionCount() == partitions
                && input.distribution().type() == DistributionType.HASH
                && joinKeys.equals(edgeKeys) && joinKeys.equals(input.distribution().keys());
    }

    private static boolean sameHashTypes(RelNode left, RelNode right,
            List<Integer> leftKeys, List<Integer> rightKeys) {
        var factory = left.getCluster().getTypeFactory();
        for (int key = 0; key < leftKeys.size(); key++) {
            var leftType = left.getRowType().getFieldList().get(leftKeys.get(key)).getType();
            var rightType = right.getRowType().getFieldList().get(rightKeys.get(key)).getType();
            if (!SqlTypeUtil.equalSansNullability(factory, leftType, rightType)) return false;
        }
        return true;
    }

    private static ImmutableBitSet preservedHashKeys(RelNode node, String exchangeId, ImmutableBitSet hashKeys) {
        if (node instanceof PangrovaExchangeInput input) {
            return input.exchangeId().equals(exchangeId) && input.requireComplete()
                    && input.distribution().type() == DistributionType.HASH
                    && ImmutableBitSet.of(input.distribution().keys()).equals(hashKeys) ? hashKeys : null;
        }
        if (node instanceof Filter filter) {
            return localExpressions(filter) ? preservedHashKeys(filter.getInput(), exchangeId, hashKeys) : null;
        }
        if (node instanceof Project project && localExpressions(project)) {
            var keys = preservedHashKeys(project.getInput(), exchangeId, hashKeys);
            if (keys == null) return null;
            var carried = ImmutableBitSet.builder();
            var outputs = ImmutableBitSet.builder();
            for (int output = 0; output < project.getProjects().size(); output++) {
                if (project.getProjects().get(output) instanceof RexInputRef reference
                        && keys.get(reference.getIndex()) && !carried.get(reference.getIndex())) {
                    carried.set(reference.getIndex());
                    outputs.set(output);
                }
            }
            return carried.build().equals(keys) ? outputs.build() : null;
        }
        if (node instanceof Aggregate aggregate && aggregate.getGroupSets().size() == 1
                && aggregate.getAggCallList().stream().allMatch(call ->
                        call.getAggregation().isDeterministic()
                                && call.getCollation().getFieldCollations().isEmpty())) {
            var keys = preservedHashKeys(aggregate.getInput(), exchangeId, hashKeys);
            var group = aggregate.getGroupSet();
            if (keys == null || !group.contains(keys)) return null;
            var outputs = ImmutableBitSet.builder();
            for (int key : keys) {
                outputs.set(group.indexOf(key));
            }
            return outputs.build();
        }
        return null;
    }

    private static boolean localExpressions(RelNode relation) {
        ExpressionInspector inspector = new ExpressionInspector();
        relation.accept(inspector);
        return inspector.local;
    }

    private static final class ExpressionInspector extends RexShuttle {
        private boolean local = true;

        @Override
        public org.apache.calcite.rex.RexNode visitSubQuery(RexSubQuery query) {
            local = false;
            return query;
        }

        @Override
        public org.apache.calcite.rex.RexNode visitOver(RexOver window) {
            local = false;
            return window;
        }

        @Override
        public org.apache.calcite.rex.RexNode visitCall(RexCall call) {
            if (!call.getOperator().isDeterministic()) {
                local = false;
            }
            return super.visitCall(call);
        }
    }
}
