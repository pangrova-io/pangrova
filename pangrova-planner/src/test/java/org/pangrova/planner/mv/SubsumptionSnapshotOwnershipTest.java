package org.pangrova.planner.mv;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubsumptionSnapshotOwnershipTest {
    @Test
    void callerMutationCannotRemoveCompensatingViewOperations() {
        var columns = new HashSet<>(List.of("id"));
        var rollup = new HashSet<>(List.of("group_id"));
        var predicate = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).makeLiteral(false);
        var filters = new ArrayList<org.apache.calcite.rex.RexNode>(List.of(predicate));
        var result = new SubsumptionResult("view", columns, filters, rollup);
        columns.clear();
        filters.clear();
        rollup.clear();
        assertThat(result.projectedColumns()).containsExactly("id");
        assertThat(result.residualFilters()).containsExactly(predicate);
        assertThat(result.rollupColumns()).containsExactly("group_id");
        assertThat(result.isExactMatch()).isFalse();
        assertThat(result.needsProjection()).isTrue();
        assertThat(result.needsFilter()).isTrue();
        assertThat(result.needsRollup()).isTrue();
        assertThatThrownBy(() -> result.projectedColumns().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.residualFilters().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.rollupColumns().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
