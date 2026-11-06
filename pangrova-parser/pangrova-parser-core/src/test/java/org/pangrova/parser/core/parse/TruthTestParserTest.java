package org.pangrova.parser.core.parse;

import org.pangrova.parser.core.format.AstPrinter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.ast.ExpressionType;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.expression.NotExpression;
import org.pangrova.parser.api.ast.expression.TruthTestExpression;

import static org.assertj.core.api.Assertions.assertThat;

class TruthTestParserTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @ParameterizedTest
    @ValueSource(strings = {"TRUE", "NOT TRUE", "FALSE", "NOT FALSE", "UNKNOWN", "NOT UNKNOWN"})
    void preservesTruthTestAndRoundTrip(String suffix) {
        var statement = parser.parse("SELECT amount > 0 IS " + suffix + " FROM orders");
        var truth = (TruthTestExpression) first((Query) statement);
        assertThat(truth.type()).isEqualTo(ExpressionType.TRUTH_TEST);
        assertThat(truth.operand()).isInstanceOf(BinaryExpression.class);
        assertThat(truth.negated()).isEqualTo(suffix.startsWith("NOT "));
        assertThat(truth.value().name()).isEqualTo(suffix.replace("NOT ", ""));
        assertThat(parser.parse(new AstPrinter().print(statement))).isEqualTo(statement);
    }

    @Test
    void truthTestBindsBeforeLogicalNotAndAnd() {
        var expression = (BinaryExpression) first((Query) parser.parse(
                "SELECT NOT flag IS TRUE AND other_flag IS NOT FALSE FROM orders"));
        assertThat(expression.operator()).isEqualTo(BinaryExpression.Operator.AND);
        assertThat(((NotExpression) expression.left()).expression()).isInstanceOf(TruthTestExpression.class);
        assertThat(expression.right()).isInstanceOf(TruthTestExpression.class);
    }

    @Test
    void unknownRemainsUsableAsAnIdentifier() {
        var statement = parser.parse("SELECT unknown FROM unknown");
        assertThat(parser.parse(new AstPrinter().print(statement))).isEqualTo(statement);
    }

    private static Expression first(Query query) {
        return ((ExpressionItem) query.selectItems().get(0)).expression();
    }
}
