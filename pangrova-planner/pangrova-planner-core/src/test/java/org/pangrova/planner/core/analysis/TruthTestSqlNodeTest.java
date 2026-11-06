package org.pangrova.planner.core.analysis;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.planner.core.parse.PangrovaParserFactory;

import static org.assertj.core.api.Assertions.assertThat;

class TruthTestSqlNodeTest {
    @ParameterizedTest
    @CsvSource({"TRUE,IS_TRUE", "NOT TRUE,IS_NOT_TRUE", "FALSE,IS_FALSE", "NOT FALSE,IS_NOT_FALSE",
            "UNKNOWN,IS_NULL", "NOT UNKNOWN,IS_NOT_NULL"})
    void usesCalciteTruthOperators(String suffix, SqlKind kind) {
        var query = (SqlSelect) new PangrovaParserFactory().parseToSqlNode(
                "SELECT amount > 0 IS " + suffix + " FROM orders");
        var expression = (SqlCall) query.getSelectList().get(0);
        assertThat(expression.getKind()).isEqualTo(kind);
        assertThat(expression.operand(0).getKind()).isEqualTo(SqlKind.GREATER_THAN);
    }
}
