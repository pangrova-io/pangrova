package org.pangrova.planner.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Keeps physical target rows intact while protecting mutation expressions. */
final class MutationPolicyInputs {
    private MutationPolicyInputs() {}

    static boolean modifiesExistingRows(LogicalTableModify modify) {
        return modify.getOperation() == TableModify.Operation.DELETE
                || modify.getOperation() == TableModify.Operation.UPDATE
                || modify.getOperation() == TableModify.Operation.MERGE;
    }

    static RelNode rowFilters(LogicalTableModify modify,
            Function<RelNode, RelNode> sourceTransform,
            Function<TableScan, RelNode> targetTransform) {
        RelNode input = modify.getInput();
        if (modify.getOperation() == TableModify.Operation.MERGE) {
            LogicalProject project = mergeProject(input);
            LogicalJoin join = (LogicalJoin) project.getInput();
            var target = PhysicalMutationTarget.extract(join.getRight());
            RelNode protectedTarget = targetTransform.apply(target.scan());
            RexNode condition = restrictTargetPredicates(join, target);
            if (protectedTarget instanceof LogicalFilter filter) {
                RexNode policy = shifted(filter.getCondition(), join.getLeft().getRowType().getFieldCount());
                condition = join.getCluster().getRexBuilder().makeCall(
                        SqlStdOperatorTable.AND, condition, policy);
            }
            LogicalJoin protectedJoin = join.copy(join.getTraitSet(), condition,
                    sourceTransform.apply(join.getLeft()), target.relation(),
                    join.getJoinType(), join.isSemiJoinDone());
            input = project.copy(project.getTraitSet(), protectedJoin,
                    project.getProjects(), project.getRowType());
        } else {
            input = compactFilters(sourceTransform.apply(input));
        }
        return modify.copy(modify.getTraitSet(), List.of(input));
    }

    static RelNode columnMasks(LogicalTableModify modify,
            Function<RelNode, RelNode> sourceTransform,
            Function<TableScan, RelNode> targetTransform) {
        RelNode input = modify.getInput();
        if (modify.getOperation() == TableModify.Operation.MERGE) {
            LogicalProject project = mergeProject(input);
            LogicalJoin join = (LogicalJoin) project.getInput();
            var target = PhysicalMutationTarget.extract(join.getRight());
            int offset = join.getLeft().getRowType().getFieldCount();
            RexShuttle masks = maskReferences(targetTransform.apply(target.scan()), offset);
            LogicalJoin protectedJoin = join.copy(join.getTraitSet(),
                    restrictTargetPredicates(join, target).accept(masks),
                    sourceTransform.apply(join.getLeft()), target.relation(),
                    join.getJoinType(), join.isSemiJoinDone());
            List<String> updateColumns = modify.getUpdateColumnList();
            int updates = updateColumns == null ? 0 : updateColumns.size();
            input = projectWithMaskedTail(project, protectedJoin,
                    project.getProjects().size() - updates, masks);
        } else {
            RelNode candidate = input instanceof LogicalProject project ? project.getInput() : input;
            TableScan target = scanUnderFilters(candidate);
            RexShuttle masks = maskReferences(targetTransform.apply(target), 0);
            RelNode protectedInput = maskFilterConditions(candidate, masks);
            input = input instanceof LogicalProject project
                    ? projectWithMaskedTail(project, protectedInput, target.getRowType().getFieldCount(), masks)
                    : protectedInput;
        }
        return modify.copy(modify.getTraitSet(), List.of(input));
    }

    private static LogicalProject mergeProject(RelNode input) {
        if (!(input instanceof LogicalProject project) || !(project.getInput() instanceof LogicalJoin)) {
            throw new IllegalArgumentException("Protected MERGE requires a project over its source/target join");
        }
        return project;
    }

    private static RexNode restrictTargetPredicates(LogicalJoin join, PhysicalMutationTarget target) {
        RexNode condition = join.getCondition();
        int offset = join.getLeft().getRowType().getFieldCount();
        for (RexNode predicate : target.matchPredicates()) {
            condition = join.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.AND, condition, shifted(predicate, offset));
        }
        return condition;
    }

    private static TableScan scanUnderFilters(RelNode input) {
        RelNode candidate = input;
        while (candidate instanceof LogicalFilter filter) {
            candidate = filter.getInput();
        }
        return PhysicalMutationTarget.extract(candidate).scan();
    }

    private static RelNode compactFilters(RelNode input) {
        if (input instanceof LogicalProject project) {
            RelNode child = compactFilters(project.getInput());
            return project.copy(project.getTraitSet(), child, project.getProjects(), project.getRowType());
        }
        if (!(input instanceof LogicalFilter filter)) {
            return input;
        }
        RexNode condition = filter.getCondition();
        RelNode child = filter.getInput();
        while (child instanceof LogicalFilter nested) {
            condition = filter.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.AND, condition, nested.getCondition());
            child = nested.getInput();
        }
        return filter.copy(filter.getTraitSet(), child, condition);
    }

    private static RexShuttle maskReferences(RelNode maskedScan, int offset) {
        List<RexNode> masks = maskedScan instanceof LogicalProject project ? project.getProjects() : List.of();
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int index = ref.getIndex() - offset;
                return index >= 0 && index < masks.size() ? shifted(masks.get(index), offset) : ref;
            }
        };
    }

    private static RexNode shifted(RexNode expression, int offset) {
        return expression.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexInputRef(ref.getIndex() + offset, ref.getType());
            }
        });
    }

    private static RelNode maskFilterConditions(RelNode input, RexShuttle masks) {
        if (!(input instanceof LogicalFilter filter)) {
            return input;
        }
        return filter.copy(filter.getTraitSet(), maskFilterConditions(filter.getInput(), masks),
                filter.getCondition().accept(masks));
    }

    private static LogicalProject projectWithMaskedTail(LogicalProject project,
            RelNode input, int start, RexShuttle masks) {
        List<RexNode> expressions = new ArrayList<>(project.getProjects());
        for (int i = start; i < expressions.size(); i++) {
            expressions.set(i, expressions.get(i).accept(masks));
        }
        return project.copy(project.getTraitSet(), input, expressions, project.getRowType());
    }
}
