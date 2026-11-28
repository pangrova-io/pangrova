/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.parser.syntax;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SelectItem;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.parser.api.ast.expression.NotExpression;
import org.pangrova.parser.api.ast.expression.StarFunctionCall;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL:2003 aggregate FILTER clause parsing: {@code agg(...) FILTER (WHERE condition)}.
 */
class AggregateFilterClauseParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    private Expression firstSelectExpression(String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).isInstanceOf(Query.class);
        SelectItem item = ((Query) stmt).selectItems().get(0);
        return ((org.pangrova.parser.api.ast.ExpressionItem) item).expression();
    }

    @Test
    void countStarWithFilterClause() {
        Expression expr = firstSelectExpression(
                "SELECT COUNT(*) FILTER (WHERE x > 1) FROM t");
        assertThat(expr).isInstanceOf(StarFunctionCall.class);
        StarFunctionCall call = (StarFunctionCall) expr;
        assertThat(call.filter()).isPresent();
        assertThat(call.window()).isEmpty();
    }

    @Test
    void sumWithNotConditionFilterClause() {
        Expression expr = firstSelectExpression(
                "SELECT SUM(x) FILTER (WHERE NOT flag) FROM t");
        assertThat(expr).isInstanceOf(FunctionCall.class);
        FunctionCall call = (FunctionCall) expr;
        assertThat(call.filter()).isPresent();
        assertThat(call.filter().get()).isInstanceOf(NotExpression.class);
        assertThat(call.distinct()).isFalse();
    }

    @Test
    void countDistinctWithFilterClause() {
        Expression expr = firstSelectExpression(
                "SELECT COUNT(DISTINCT x) FILTER (WHERE y = 2) FROM t");
        assertThat(expr).isInstanceOf(FunctionCall.class);
        FunctionCall call = (FunctionCall) expr;
        assertThat(call.distinct()).isTrue();
        assertThat(call.filter()).isPresent();
    }

    @Test
    void filterConditionAstShapeIsPreserved() {
        Expression expr = firstSelectExpression(
                "SELECT COUNT(*) FILTER (WHERE price > 100) FROM orders");
        StarFunctionCall call = (StarFunctionCall) expr;
        assertThat(call.filter().get()).isInstanceOf(BinaryExpression.class);
    }

    @Test
    void aggregateWithoutFilterHasEmptyFilter() {
        Expression expr = firstSelectExpression("SELECT COUNT(*) FROM t");
        assertThat(((StarFunctionCall) expr).filter()).isEmpty();

        Expression sum = firstSelectExpression("SELECT SUM(x) FROM t");
        assertThat(((FunctionCall) sum).filter()).isEmpty();
    }

    @Test
    void filterRemainsUsableAsIdentifier() {
        // FILTER is non-reserved: existing queries using it as a column or
        // table name must keep parsing.
        Statement columnUse = parser.parse("SELECT filter FROM t");
        assertThat(columnUse).isInstanceOf(Query.class);

        Statement tableUse = parser.parse("SELECT f.filter FROM filter f");
        assertThat(tableUse).isInstanceOf(Query.class);
    }

    @Test
    void filterCombinedWithOverIsRejectedWithTargetedMessage() {
        assertThatThrownBy(() -> parser.parse(
                "SELECT COUNT(*) FILTER (WHERE x > 1) OVER (PARTITION BY y) FROM t"))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_SYNTAX_ERROR)
                .hasMessageContaining("FILTER (WHERE ...) combined with OVER");
    }

    @Test
    void filterOnArgumentAggregateCombinedWithOverIsRejected() {
        assertThatThrownBy(() -> parser.parse(
                "SELECT SUM(x) FILTER (WHERE x > 1) OVER (PARTITION BY y) FROM t"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("FILTER (WHERE ...) combined with OVER");
    }
}
