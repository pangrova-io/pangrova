package org.pangrova.parser.syntax;

import org.pangrova.parser.api.enums.UnpivotNullHandling;
import org.pangrova.parser.api.enums.TableRefKind;
import org.pangrova.parser.api.ast.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for PIVOT and UNPIVOT SQL syntax parsing per SQL:2016 standard.
 */
class PivotUnpivotParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    // ── PIVOT ──

    @Test
    void parsePivotSalesByQuarter() {
        Statement stmt = parser.parse(
                "SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.from()).hasSize(1);
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.PIVOT);
        assertThat(ref.pivotClause()).isPresent();
        PivotClause pivot = ref.pivotClause().get();
        assertThat(pivot.aggregates()).hasSize(1);
        assertThat(pivot.aggregates().get(0).function().parts()).containsExactly("SUM");
        assertThat(pivot.forColumn().parts()).containsExactly("quarter");
        assertThat(pivot.inValues()).hasSize(4);
    }

    @Test
    void parsePivotWithMultipleAggregates() {
        Statement stmt = parser.parse(
                "SELECT * FROM sales PIVOT ("
                + "SUM(amount) AS total, COUNT(amount) AS cnt "
                + "FOR region IN ('East', 'West'))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.PIVOT);
        PivotClause pivot = ref.pivotClause().get();
        assertThat(pivot.aggregates()).hasSize(2);
        assertThat(pivot.aggregates().get(0).alias()).contains("total");
        assertThat(pivot.aggregates().get(1).alias()).contains("cnt");
    }

    @Test
    void parsePivotWithAlias() {
        Statement stmt = parser.parse(
                "SELECT * FROM orders PIVOT (SUM(qty) FOR status IN ('open', 'closed')) AS p");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.PIVOT);
        assertThat(ref.alias()).contains("p");
    }

    @Test
    void parsePivotWithValueAliases() {
        Statement stmt = parser.parse(
                "SELECT * FROM data PIVOT (AVG(score) FOR year IN (2021 AS y21, 2022 AS y22))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        PivotClause pivot = ref.pivotClause().get();
        assertThat(pivot.inValues()).hasSize(2);
        assertThat(pivot.inValues().get(0).alias()).contains("y21");
        assertThat(pivot.inValues().get(1).alias()).contains("y22");
    }

    // ── UNPIVOT ──

    @Test
    void parseUnpivotColumnsToRows() {
        Statement stmt = parser.parse(
                "SELECT * FROM quarterly_sales UNPIVOT (amount FOR quarter IN (q1, q2, q3, q4))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.from()).hasSize(1);
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.UNPIVOT);
        assertThat(ref.unpivotClause()).isPresent();
        UnpivotClause unpivot = ref.unpivotClause().get();
        assertThat(unpivot.valueColumn().parts()).containsExactly("amount");
        assertThat(unpivot.nameColumn().parts()).containsExactly("quarter");
        assertThat(unpivot.sourceColumns()).hasSize(4);
        assertThat(unpivot.nullHandling()).isEqualTo(UnpivotNullHandling.EXCLUDE);
    }

    @Test
    void parseUnpivotIncludeNulls() {
        Statement stmt = parser.parse(
                "SELECT * FROM t UNPIVOT INCLUDE NULLS (val FOR col_name IN (a, b, c))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        UnpivotClause unpivot = ref.unpivotClause().get();
        assertThat(unpivot.nullHandling()).isEqualTo(UnpivotNullHandling.INCLUDE);
        assertThat(unpivot.sourceColumns()).hasSize(3);
    }

    @Test
    void parseUnpivotExcludeNulls() {
        Statement stmt = parser.parse(
                "SELECT * FROM t UNPIVOT EXCLUDE NULLS (val FOR col_name IN (x, y))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        UnpivotClause unpivot = ref.unpivotClause().get();
        assertThat(unpivot.nullHandling()).isEqualTo(UnpivotNullHandling.EXCLUDE);
    }

    @Test
    void parseUnpivotWithAlias() {
        Statement stmt = parser.parse(
                "SELECT * FROM t UNPIVOT (value FOR name IN (col1, col2)) AS u");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.kind()).isEqualTo(TableRefKind.UNPIVOT);
        assertThat(ref.alias()).contains("u");
    }

    @Test
    void parseUnpivotWithColumnAliases() {
        Statement stmt = parser.parse(
                "SELECT * FROM t UNPIVOT (val FOR metric IN (revenue AS 'Revenue', cost AS 'Cost'))");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        UnpivotClause unpivot = ref.unpivotClause().get();
        assertThat(unpivot.sourceColumns()).hasSize(2);
        assertThat(unpivot.sourceColumns().get(0).alias()).contains("Revenue");
        assertThat(unpivot.sourceColumns().get(1).alias()).contains("Cost");
    }

    // ── Non-reserved keyword usage ──

    @Test
    void pivotAsIdentifier() {
        Statement stmt = parser.parse("SELECT pivot FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.selectItems()).hasSize(1);
    }

    @Test
    void unpivotAsIdentifier() {
        Statement stmt = parser.parse("SELECT unpivot FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.selectItems()).hasSize(1);
    }
}
