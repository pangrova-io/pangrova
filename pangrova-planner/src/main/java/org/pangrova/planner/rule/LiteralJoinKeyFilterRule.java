package org.pangrova.planner.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/** Restricts a scan to the complete integer key domain of a small literal equijoin input. */
public final class LiteralJoinKeyFilterRule extends RelOptRule {
    public static final LiteralJoinKeyFilterRule INSTANCE = new LiteralJoinKeyFilterRule();
    private static final int MAX_BUILD_ROWS = 128;

    private LiteralJoinKeyFilterRule() {
        super(operand(LogicalJoin.class, any()), "LiteralJoinKeyFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        if ((join.getJoinType() != JoinRelType.INNER && join.getJoinType() != JoinRelType.SEMI)
                || !join.getSystemFieldList().isEmpty()
                || !(join.getCondition() instanceof RexCall equality) || equality.getKind() != SqlKind.EQUALS
                || !(equality.operands.get(0) instanceof RexInputRef first)
                || !(equality.operands.get(1) instanceof RexInputRef second)) {
            return;
        }
        int probeWidth = join.getLeft().getRowType().getFieldCount();
        int probeKey = Math.min(first.getIndex(), second.getIndex());
        int buildKey = Math.max(first.getIndex(), second.getIndex()) - probeWidth;
        var type = first.getType().getSqlTypeName();
        if (probeKey >= probeWidth || buildKey < 0 || type != second.getType().getSqlTypeName()
                || !SqlTypeName.INT_TYPES.contains(type)) {
            return;
        }
        var probePath = traceKey(join.getLeft(), probeKey);
        var buildPath = traceKey(join.getRight(), buildKey);
        if (probePath == null || !(probePath.input() instanceof LogicalTableScan scan)
                || buildPath == null || !(buildPath.input() instanceof LogicalValues build)
                || build.getTuples().size() > MAX_BUILD_ROWS) {
            return;
        }
        var keys = new LinkedHashSet<RexLiteral>();
        for (var tuple : build.getTuples()) {
            var value = tuple.get(buildPath.ordinal());
            if (!value.isNull()) {
                keys.add(value);
            }
        }
        var rex = join.getCluster().getRexBuilder();
        RexNode predicate = keys.isEmpty() ? rex.makeLiteral(false)
                : rex.makeIn(rex.makeInputRef(scan, probePath.ordinal()), List.copyOf(keys));
        RelNode probe = LogicalFilter.create(scan, predicate);
        var projects = probePath.projects();
        for (int i = projects.size() - 1; i >= 0; i--) {
            var project = projects.get(i);
            probe = project.copy(project.getTraitSet(), List.of(probe));
        }
        // Retain the join and the complete build relation, including duplicate and null keys.
        call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), probe, join.getRight(),
                join.getJoinType(), join.isSemiJoinDone()));
    }

    private record KeyPath(RelNode input, int ordinal, List<Project> projects) {}

    /** Trace a renamed key or a nullability-only cast through deterministic projections. */
    private static KeyPath traceKey(RelNode node, int ordinal) {
        var projects = new ArrayList<Project>();
        while (true) {
            if (node instanceof HepRelVertex vertex) {
                node = vertex.getCurrentRel();
            }
            if (ordinal < 0 || ordinal >= node.getRowType().getFieldCount()) {
                return null;
            }
            if (!(node instanceof Project project)) {
                return new KeyPath(node, ordinal, projects);
            }
            if (!project.getProjects().stream().allMatch(RexUtil::isDeterministic)) {
                return null;
            }
            RexNode expression = project.getProjects().get(ordinal);
            if (expression instanceof RexCall cast && cast.getKind() == SqlKind.CAST
                    && cast.getType().getSqlTypeName() == cast.operands.get(0).getType().getSqlTypeName()) {
                expression = cast.operands.get(0);
            }
            if (!(expression instanceof RexInputRef input)) {
                return null;
            }
            projects.add(project);
            node = project.getInput();
            ordinal = input.getIndex();
        }
    }
}
