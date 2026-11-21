package org.pangrova.parser.parse;


import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.Statement;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for the DECIMAL-literal precision-loss defect: literals were parsed via
 * {@code Double.parseDouble}, irreversibly truncating values beyond double's ~15–17
 * significant digits (so DECIMAL(38,s) literals could not survive planning/DML).
 * The fix stores the raw lexeme as {@link BigDecimal}; these tests assert exact
 * round-trip for >18-digit and full 38-digit literals.
 */
class DecimalLiteralPrecisionTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    private BigDecimal parseDecimal(String literalText) {
        Statement stmt = parser.parse("SELECT " + literalText + " FROM t");
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(Literal.class);
        LiteralValue value = ((Literal) expr).value();
        assertThat(value).isInstanceOf(LiteralValue.DecimalValue.class);
        return ((LiteralValue.DecimalValue) value).value();
    }

    @Test
    void decimalWithMoreThanEighteenDigitsRoundTripsExactly() {
        String text = "123456789012345678901234567.123456789";
        BigDecimal parsed = parseDecimal(text);
        // Exact match: the same digits/scale we wrote, not a double-rounded approximation.
        assertThat(parsed).isEqualByComparingTo(new BigDecimal(text));
        assertThat(parsed.toPlainString()).isEqualTo(text);
        // Prove the old double path would have lost precision.
        assertThat(BigDecimal.valueOf(Double.parseDouble(text)).toPlainString())
                .isNotEqualTo(text);
    }

    @Test
    void fullThirtyEightDigitDecimalRoundTripsExactly() {
        // DECIMAL(38, 4): 34 integer + 4 fractional nines — far beyond double's precision.
        String text = "9999999999999999999999999999999999.9999";
        BigDecimal parsed = parseDecimal(text);
        assertThat(parsed).isEqualByComparingTo(new BigDecimal(text));
        assertThat(parsed.toPlainString()).isEqualTo(text);
        assertThat(parsed.precision()).isEqualTo(38);
        assertThat(parsed.scale()).isEqualTo(4);
    }

    @Test
    void negativeHighPrecisionDecimalRoundTripsExactly() {
        String text = "-12345678901234567890.0987654321";
        BigDecimal parsed = parseDecimal(text);
        assertThat(parsed).isEqualByComparingTo(new BigDecimal(text));
        assertThat(parsed.toPlainString()).isEqualTo(text);
        assertThat(parsed.signum()).isEqualTo(-1);
    }

    @Test
    void scientificNotationPreservesExactDecimalValue() {
        for (String text : new String[] {"3.3283234E-4", "-6.222725e-5", "1E+3", ".25e2", "4."}) {
            assertThat(parseDecimal(text)).isEqualByComparingTo(new BigDecimal(text));
        }
    }
}
