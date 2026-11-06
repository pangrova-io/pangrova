package org.pangrova.planner.core.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryPlanOwnershipTest {
    @Test
    void eachQueryOwnsItsClusterWhilePlannerConsumersBorrowTheSameTree() {
        var planner = new PangrovaPlanner(CalciteSchema.createRootSchema(false), List.of(""));
        var first = planner.plan("SELECT 1 AS first_value");
        var second = planner.plan("SELECT 2 AS second_value");
        assertThat(second.plan()).isNotSameAs(first.plan());
        assertThat(second.plan().getCluster()).isNotSameAs(first.plan().getCluster());
        var query = first.toPlannedQuery();
        var fragment = new PlanFragment(first.plan(), 0, 0);
        assertThat(query.plan()).isSameAs(first.plan());
        assertThat(fragment.relNode()).isSameAs(first.plan());
        assertThat(query.columnNames()).containsExactly("first_value");
        assertThat(second.preOptimizationColumnNames()).containsExactly("second_value");
        assertThatThrownBy(() -> query.columnNames().add("mutated"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> query.runtimeFilterDescriptors().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> query.physicalToLogicalDynamicTables().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
