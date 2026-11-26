package org.pangrova.planner.analysis;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;

import java.util.List;

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

    private static ImmutableBitSet preservedHashKeys(RelNode node, String exchangeId, ImmutableBitSet hashKeys) {
        if (node instanceof PangrovaExchangeInput input) {
            return input.exchangeId().equals(exchangeId) && input.requireComplete()
                    && input.distribution().type() == org.pangrova.planner.enums.DistributionType.HASH
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
