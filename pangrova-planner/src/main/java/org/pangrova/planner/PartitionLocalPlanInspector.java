package org.pangrova.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

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
        ExpressionInspector inspector = new ExpressionInspector();
        relation.accept(inspector);
        return inspector.local && relation.getInputs().size() == 1
                && isIndependent(relation.getInput(0));
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
