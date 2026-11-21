package org.pangrova.planner.cost.join;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;
import java.util.Set;

/**
 * Bundles the parameters required to construct an {@link PangrovaSortMergeJoin}.
 * Reduces the constructor parameter count to comply with the max-4-params quality gate.
 */
public record SortMergeJoinConfig(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType) {

    public SortMergeJoinConfig {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(traitSet, "traitSet");
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(condition, "condition");
        variablesSet = Set.copyOf(variablesSet);
        Objects.requireNonNull(joinType, "joinType");
    }
}
