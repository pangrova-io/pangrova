package org.pangrova.planner.core.analysis;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.util.TimestampString;
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

    @Test
    void timestampLiteralKeepsNanosecondsThroughParsingAndPlanning() {
        RelNode result = planner.plan(
                "SELECT TIMESTAMP '2025-01-01 00:00:00.123456789'").plan();

        assertThat(result.getRowType().getFieldList().get(0).getType().getPrecision())
                .isEqualTo(9);
        assertThat(result).isInstanceOf(Values.class);
        var literal = ((Values) result).getTuples().get(0).get(0);
        assertThat(literal.getValueAs(TimestampString.class))
                .isEqualTo(new TimestampString("2025-01-01 00:00:00.123456789"));
    }
}
