package org.pangrova.parser.parse;


import org.pangrova.parser.api.ast.*;
import org.pangrova.parser.api.ast.expression.WindowedFunctionCall;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for NAMED WINDOW definitions and RANGE frame type parsing.
 */
class NamedWindowAndRangeFrameTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    // ── NAMED WINDOW referenced by multiple functions ──

    @Test
    void parseNamedWindowReferencedByMultipleFunctions() {
        Statement stmt = parser.parse(
                "SELECT SUM(amount) OVER w, AVG(amount) OVER w, ROW_NUMBER() OVER w "
                + "FROM orders "
                + "WINDOW w AS (PARTITION BY customer_id ORDER BY order_date)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        // Verify WINDOW clause has one definition
        assertThat(q.windowDefinitions()).hasSize(1);
        NamedWindow w = q.windowDefinitions().get(0);
        assertThat(w.name()).isEqualTo("w");
        assertThat(w.partitionBy()).hasSize(1);
        assertThat(w.orderBy()).hasSize(1);
        assertThat(w.frame()).isEmpty();

        // Verify all three functions reference the named window
        assertThat(q.selectItems()).hasSize(3);
        for (SelectItem item : q.selectItems()) {
            assertThat(item).isInstanceOf(ExpressionItem.class);
            Expression expr = ((ExpressionItem) item).expression();
            assertThat(expr).isInstanceOf(WindowedFunctionCall.class);
            WindowedFunctionCall wfc = (WindowedFunctionCall) expr;
            assertThat(wfc.window().windowName()).isPresent();
            assertThat(wfc.window().windowName().get()).isEqualTo("w");
        }
    }

    @Test
    void parseMultipleNamedWindows() {
        Statement stmt = parser.parse(
                "SELECT SUM(x) OVER w1, AVG(y) OVER w2 FROM t "
                + "WINDOW w1 AS (PARTITION BY a ORDER BY b), "
                + "w2 AS (PARTITION BY c ORDER BY d DESC)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        assertThat(q.windowDefinitions()).hasSize(2);
        assertThat(q.windowDefinitions().get(0).name()).isEqualTo("w1");
        assertThat(q.windowDefinitions().get(1).name()).isEqualTo("w2");
    }

    @Test
    void parseNamedWindowWithFrame() {
        Statement stmt = parser.parse(
                "SELECT SUM(x) OVER w FROM t "
                + "WINDOW w AS (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        assertThat(q.windowDefinitions()).hasSize(1);
        NamedWindow w = q.windowDefinitions().get(0);
        assertThat(w.frame()).isPresent();
        assertThat(w.frame().get().frameType()).isEqualTo(WindowSpec.FrameType.ROWS);
    }

    // ── RANGE frame type ──

    @Test
    void parseRangeFrameBetween() {
        Statement stmt = parser.parse(
                "SELECT SUM(amount) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        SelectItem item = q.selectItems().get(0);
        Expression expr = ((ExpressionItem) item).expression();
        assertThat(expr).isInstanceOf(WindowedFunctionCall.class);
        WindowedFunctionCall wfc = (WindowedFunctionCall) expr;

        assertThat(wfc.window().frame()).isPresent();
        WindowSpec.WindowFrame frame = wfc.window().frame().get();
        assertThat(frame.frameType()).isEqualTo(WindowSpec.FrameType.RANGE);
        assertThat(frame.lowerBound().type()).isEqualTo(WindowSpec.FrameBoundType.UNBOUNDED_PRECEDING);
        assertThat(frame.upperBound().type()).isEqualTo(WindowSpec.FrameBoundType.CURRENT_ROW);
    }

    @Test
    void parseRangeFrameWithNumericOffset() {
        Statement stmt = parser.parse(
                "SELECT AVG(price) OVER (ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 5 FOLLOWING) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        SelectItem item = q.selectItems().get(0);
        Expression expr = ((ExpressionItem) item).expression();
        WindowedFunctionCall wfc = (WindowedFunctionCall) expr;

        assertThat(wfc.window().frame()).isPresent();
        WindowSpec.WindowFrame frame = wfc.window().frame().get();
        assertThat(frame.frameType()).isEqualTo(WindowSpec.FrameType.RANGE);
        assertThat(frame.lowerBound().type()).isEqualTo(WindowSpec.FrameBoundType.EXPR_PRECEDING);
        assertThat(frame.lowerBound().offset()).isPresent();
        assertThat(frame.upperBound().type()).isEqualTo(WindowSpec.FrameBoundType.EXPR_FOLLOWING);
        assertThat(frame.upperBound().offset()).isPresent();
    }

    @Test
    void parseRangeFrameWithIntervalOffset() {
        Statement stmt = parser.parse(
                "SELECT SUM(amount) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        SelectItem item = q.selectItems().get(0);
        Expression expr = ((ExpressionItem) item).expression();
        WindowedFunctionCall wfc = (WindowedFunctionCall) expr;

        assertThat(wfc.window().frame()).isPresent();
        WindowSpec.WindowFrame frame = wfc.window().frame().get();
        assertThat(frame.frameType()).isEqualTo(WindowSpec.FrameType.RANGE);
        assertThat(frame.lowerBound().type()).isEqualTo(WindowSpec.FrameBoundType.EXPR_PRECEDING);
        assertThat(frame.lowerBound().offset()).isPresent();
    }

    @Test
    void parseNamedWindowWithRangeFrame() {
        Statement stmt = parser.parse(
                "SELECT SUM(x) OVER w FROM t "
                + "WINDOW w AS (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        assertThat(q.windowDefinitions()).hasSize(1);
        NamedWindow w = q.windowDefinitions().get(0);
        assertThat(w.frame()).isPresent();
        assertThat(w.frame().get().frameType()).isEqualTo(WindowSpec.FrameType.RANGE);
        assertThat(w.frame().get().lowerBound().type()).isEqualTo(WindowSpec.FrameBoundType.UNBOUNDED_PRECEDING);
        assertThat(w.frame().get().upperBound().type()).isEqualTo(WindowSpec.FrameBoundType.UNBOUNDED_FOLLOWING);
    }

    // ── Existing ROWS frame still works ──

    @Test
    void parseRowsFrameStillWorks() {
        Statement stmt = parser.parse(
                "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;

        SelectItem item = q.selectItems().get(0);
        Expression expr = ((ExpressionItem) item).expression();
        WindowedFunctionCall wfc = (WindowedFunctionCall) expr;

        assertThat(wfc.window().frame()).isPresent();
        WindowSpec.WindowFrame frame = wfc.window().frame().get();
        assertThat(frame.frameType()).isEqualTo(WindowSpec.FrameType.ROWS);
    }

    // ── WINDOW clause position (after QUALIFY, before ORDER BY) ──

    @Test
    void parseWindowClauseWithQualifyAndOrderBy() {
        Statement stmt = parser.parse(
                "SELECT SUM(x) OVER w, ROW_NUMBER() OVER w FROM t "
                + "QUALIFY ROW_NUMBER() OVER w = 1 "
                + "WINDOW w AS (PARTITION BY grp ORDER BY val) "
                + "ORDER BY val");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.qualify()).isPresent();
        assertThat(q.windowDefinitions()).hasSize(1);
        assertThat(q.orderBy()).hasSize(1);
    }
}
