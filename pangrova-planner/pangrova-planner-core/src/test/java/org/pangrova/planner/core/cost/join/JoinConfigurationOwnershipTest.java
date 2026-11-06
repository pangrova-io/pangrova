package org.pangrova.planner.core.cost.join;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JoinConfigurationOwnershipTest {
    @Test
    void joinConfigsFreezeCorrelationsButRetainPlannerOwnedFrameworkIdentity() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(),
                new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)));
        var left = LogicalValues.createOneRow(cluster);
        var right = LogicalValues.createOneRow(cluster);
        var traits = left.getTraitSet();
        var condition = cluster.getRexBuilder().makeLiteral(true);
        var correlation = new CorrelationId(7);
        var variables = new HashSet<>(List.of(correlation));
        var hash = new HashJoinConfig(cluster, traits, left, right, condition,
                variables, JoinRelType.INNER);
        var broadcast = new BroadcastJoinConfig(cluster, traits, left, right, condition,
                variables, JoinRelType.INNER);
        var nested = new NestedLoopJoinConfig(cluster, traits, left, right, condition,
                variables, JoinRelType.INNER);
        var merge = new SortMergeJoinConfig(cluster, traits, left, right, condition,
                variables, JoinRelType.INNER);
        variables.clear();
        for (var snapshot : List.of(hash.variablesSet(), broadcast.variablesSet(),
                nested.variablesSet(), merge.variablesSet())) {
            assertThat(snapshot).containsExactly(correlation);
            assertThatThrownBy(snapshot::clear).isInstanceOf(UnsupportedOperationException.class);
        }
        assertThat(List.of(hash.cluster(), broadcast.cluster(), nested.cluster(), merge.cluster()))
                .allSatisfy(value -> assertThat(value).isSameAs(cluster));
        assertThat(List.of(hash.traitSet(), broadcast.traitSet(), nested.traitSet(), merge.traitSet()))
                .allSatisfy(value -> assertThat(value).isSameAs(traits));
        assertThat(List.of(hash.left(), broadcast.left(), nested.left(), merge.left()))
                .allSatisfy(value -> assertThat(value).isSameAs(left));
        assertThat(List.of(hash.right(), broadcast.right(), nested.right(), merge.right()))
                .allSatisfy(value -> assertThat(value).isSameAs(right));
    }
}
