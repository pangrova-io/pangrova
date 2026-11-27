package org.pangrova.planner.analysis;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

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
    void stringLiteralsPreserveBmpAndSupplementaryCharactersInValues() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        for (String value : new String[] {bmp, supplementary}) {
            RelNode result = planner.plan("SELECT '" + value + "'").plan();

            assertThat(result.getRowType().getFieldList().get(0).getType().getCharset())
                    .isEqualTo(StandardCharsets.UTF_8);
            assertThat(result).isInstanceOf(Values.class);
            assertThat(((Values) result).getTuples().get(0).get(0).getValueAs(String.class))
                    .isEqualTo(value);
        }
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
