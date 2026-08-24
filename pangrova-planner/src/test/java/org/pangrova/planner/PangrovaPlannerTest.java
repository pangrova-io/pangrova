package org.pangrova.planner;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PangrovaPlanner initialization and basic planning.
 */
class PangrovaPlannerTest {

    private final PangrovaPlanner planner = new PangrovaPlanner();

    @Test
    void planSelectLiteralReturnsNonNullRelNode() {
        RelNode result = planner.plan("SELECT 1").plan();
        assertThat(result).isNotNull();
    }

    @Test
    void planSelectStringLiteralReturnsNonNullRelNode() {
        RelNode result = planner.plan("SELECT 'hello'").plan();
        assertThat(result).isNotNull();
    }

    @Test
    void planSelectArithmeticReturnsNonNullRelNode() {
        RelNode result = planner.plan("SELECT 1 + 2").plan();
        assertThat(result).isNotNull();
    }
}
