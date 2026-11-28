package org.pangrova.parser.syntax;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.parser.api.ast.SourceLocation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that invalid numeric literals in type definitions produce
 * ParseException with SourceLocation rather than escaping as raw
 * NumberFormatException to the coordinator.
 */
class NumericLiteralParseErrorTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void invalidVectorDimensionProducesParseException() {
        // Overflow value that passes lexer as INTEGER_LITERAL but fails parseInt
        String sql = "CREATE TABLE t (col VECTOR(99999999999999999999))";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .satisfies(ex -> {
                    ParseException pe = (ParseException) ex;
                    assertThat(pe.getLine()).isGreaterThan(0);
                    assertThat(pe.getColumn()).isGreaterThanOrEqualTo(0);
                    assertThat(pe.getSourceLocation()).isNotNull();
                    assertThat(pe.getCause()).isInstanceOf(NumberFormatException.class);
                });
    }

    @Test
    void invalidDecimalPrecisionProducesParseException() {
        // Overflow value that passes lexer as INTEGER_LITERAL but fails parseInt
        String sql = "CREATE TABLE t (col DECIMAL(99999999999999999999, 2))";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .satisfies(ex -> {
                    ParseException pe = (ParseException) ex;
                    assertThat(pe.getLine()).isGreaterThan(0);
                    assertThat(pe.getColumn()).isGreaterThanOrEqualTo(0);
                    assertThat(pe.getSourceLocation()).isNotNull();
                    assertThat(pe.getCause()).isInstanceOf(NumberFormatException.class);
                });
    }

    @Test
    void invalidDecimalScaleProducesParseException() {
        // Overflow value that passes lexer as INTEGER_LITERAL but fails parseInt
        String sql = "CREATE TABLE t (col DECIMAL(10, 99999999999999999999))";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .satisfies(ex -> {
                    ParseException pe = (ParseException) ex;
                    assertThat(pe.getLine()).isGreaterThan(0);
                    assertThat(pe.getColumn()).isGreaterThanOrEqualTo(0);
                    assertThat(pe.getSourceLocation()).isNotNull();
                    assertThat(pe.getCause()).isInstanceOf(NumberFormatException.class);
                });
    }

    @Test
    void invalidIntegerLiteralProducesParseException() {
        String sql = "SELECT 99999999999999999999999999999999999999";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .satisfies(ex -> {
                    ParseException pe = (ParseException) ex;
                    assertThat(pe.getLine()).isGreaterThan(0);
                    assertThat(pe.getColumn()).isGreaterThanOrEqualTo(0);
                    assertThat(pe.getSourceLocation()).isNotNull();
                    SourceLocation loc = pe.getSourceLocation();
                    assertThat(loc.line()).isEqualTo(1);
                    assertThat(loc.col()).isGreaterThanOrEqualTo(0);
                    assertThat(pe.getMessage()).containsIgnoringCase("integer literal");
                    assertThat(pe.getCause()).isInstanceOf(NumberFormatException.class);
                });
    }

    @Test
    void validDecimalTypeParseSucceeds() {
        String sql = "CREATE TABLE t (col DECIMAL(10, 2))";
        var stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();
    }

    @Test
    void validVectorTypeParseSucceeds() {
        String sql = "CREATE TABLE t (col VECTOR(128))";
        var stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();
    }
}
