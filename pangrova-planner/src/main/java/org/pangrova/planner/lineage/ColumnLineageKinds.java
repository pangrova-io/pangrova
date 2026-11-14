package org.pangrova.planner.lineage;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;

import org.pangrova.governance.api.enums.ColumnDerivationKind;

/** Propagates expression classification through actual relational outputs. */
final class ColumnLineageKinds {
    private final java.util.Map<org.apache.calcite.rel.core.CorrelationId, RelNode> correlations;

    ColumnLineageKinds(java.util.Map<org.apache.calcite.rel.core.CorrelationId, RelNode> correlations) {
        this.correlations = java.util.Map.copyOf(correlations);
    }

    ColumnDerivationKind field(RelNode relation, int output) {
        relation = unwrap(relation);
        if (output < 0 || output >= relation.getRowType().getFieldCount()) {
            throw new IllegalArgumentException("Lineage classification field is outside the actual row type");
        }
        if (relation instanceof TableScan) {
            return ColumnDerivationKind.DIRECT;
        }
        if (relation instanceof Values) {
            return ColumnDerivationKind.TRANSFORM;
        }
        if (relation instanceof TableFunctionScan function && function.getInputs().isEmpty()) {
            return ColumnDerivationKind.TRANSFORM;
        }
        if (relation instanceof Project project) {
            return expression(project.getInput(), project.getProjects().get(output));
        }
        if (relation instanceof Calc calc) {
            return expression(calc.getInput(),
                    calc.getProgram().expandLocalRef(calc.getProgram().getProjectList().get(output)));
        }
        if (relation instanceof Aggregate aggregate) {
            return output < aggregate.getGroupCount()
                    ? field(aggregate.getInput(), aggregate.getGroupSet().asList().get(output))
                    : ColumnDerivationKind.AGGREGATE;
        }
        if (relation instanceof Window window) {
            return output < window.getInput().getRowType().getFieldCount()
                    ? field(window.getInput(), output) : ColumnDerivationKind.AGGREGATE;
        }
        if (relation instanceof Join join) {
            return joinField(join, output);
        }
        if (relation instanceof org.apache.calcite.rel.core.Correlate correlate) {
            int left = correlate.getLeft().getRowType().getFieldCount();
            if (output < left) {
                return field(correlate.getLeft(), output);
            }
            var kind = field(correlate.getRight(), output - left);
            return correlate.getJoinType().generatesNullsOnRight()
                    ? combine(kind, ColumnDerivationKind.TRANSFORM) : kind;
        }
        if (relation instanceof org.apache.calcite.rel.core.Uncollect uncollect) {
            var kind = ColumnDerivationKind.TRANSFORM;
            for (int input : UncollectColumnLayout.inputs(uncollect, output)) {
                kind = combine(kind, field(uncollect.getInput(), input));
            }
            return kind;
        }
        if (relation instanceof SetOp set) {
            ColumnDerivationKind kind = ColumnDerivationKind.DIRECT;
            for (var input : set.getInputs()) {
                kind = combine(kind, field(input, output));
            }
            return kind;
        }
        if (relation instanceof org.apache.calcite.rel.core.RepeatUnion repeat) {
            return combine(field(repeat.getSeedRel(), output),
                    field(repeat.getIterativeRel(), output));
        }
        if (relation instanceof org.apache.calcite.rel.logical.LogicalTableSpool spool) {
            return field(spool.getInput(), output);
        }
        if (passthrough(relation)) {
            return field(relation.getInput(0), output);
        }
        throw new IllegalArgumentException("Column lineage has an unsupported relation: " + relation.getRelTypeName());
    }

    private ColumnDerivationKind joinField(Join join, int output) {
        int left = join.getLeft().getRowType().getFieldCount();
        ColumnDerivationKind kind = output < left ? field(join.getLeft(), output)
                : field(join.getRight(), output - left);
        boolean generatedNull = output < left ? join.getJoinType().generatesNullsOnLeft()
                : join.getJoinType().generatesNullsOnRight();
        return generatedNull ? combine(kind, ColumnDerivationKind.TRANSFORM) : kind;
    }

    private static boolean passthrough(RelNode relation) {
        return relation instanceof Filter || relation instanceof Sort || relation instanceof Exchange
                || relation instanceof Sample || relation instanceof Snapshot
                || relation instanceof org.pangrova.planner.cost.distribution.PangrovaExchange;
    }

    private ColumnDerivationKind expression(RelNode input, RexNode expression) {
        if (expression instanceof RexInputRef reference) {
            return field(input, reference.getIndex());
        }
        if (expression instanceof RexLiteral || expression instanceof RexDynamicParam) {
            return ColumnDerivationKind.TRANSFORM;
        }
        if (expression instanceof RexFieldAccess access
                && access.getReferenceExpr() instanceof org.apache.calcite.rex.RexCorrelVariable variable) {
            var owner = QueryColumnOrigins.requireCorrelationOwner(correlations, variable, access);
            return field(owner, access.getField().getIndex());
        }
        if (expression instanceof RexFieldAccess access) {
            return combine(ColumnDerivationKind.TRANSFORM, expression(input, access.getReferenceExpr()));
        }
        if (expression instanceof RexCall call && !(expression instanceof RexSubQuery)) {
            ColumnDerivationKind result = expression instanceof RexOver
                    ? ColumnDerivationKind.AGGREGATE : ColumnDerivationKind.TRANSFORM;
            for (var operand : call.getOperands()) {
                result = combine(result, expression(input, operand));
            }
            return result;
        }
        throw new IllegalArgumentException("Lineage classification has an unresolved expression");
    }

    static ColumnDerivationKind combine(ColumnDerivationKind left, ColumnDerivationKind right) {
        if (left == ColumnDerivationKind.AGGREGATE || right == ColumnDerivationKind.AGGREGATE) {
            return ColumnDerivationKind.AGGREGATE;
        }
        return left == ColumnDerivationKind.TRANSFORM || right == ColumnDerivationKind.TRANSFORM
                ? ColumnDerivationKind.TRANSFORM : ColumnDerivationKind.DIRECT;
    }

    static RelNode unwrap(RelNode relation) {
        if (relation instanceof org.apache.calcite.plan.hep.HepRelVertex vertex) {
            return unwrap(vertex.getCurrentRel());
        }
        if (relation instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            return unwrap(subset.getBestOrOriginal());
        }
        return relation;
    }
}
