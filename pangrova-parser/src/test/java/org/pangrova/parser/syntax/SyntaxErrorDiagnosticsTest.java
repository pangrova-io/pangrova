package org.pangrova.parser.syntax;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.api.syntax.ParseResult;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Verifies that raw ANTLR syntax errors are rewritten into Trino-style diagnostics
 * carrying (1) an expected-token hint, (2) a caret-annotated source snippet, and
 * (3) a stable error class / SQLSTATE classification — across both the strict
 * ({@link AntlrSqlParser#parse}) and error-recovery ({@link AntlrSqlParser#parseWithRecovery})
 * entry points.
 */
class SyntaxErrorDiagnosticsTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void strictParseSyntaxErrorHasExpectedTokenCaretAndSqlstate() {
        ParseException ex = catchThrowableOfType(
                () -> parser.parse("SELECT FROM"), ParseException.class);
        assertThat(ex).isNotNull();
        String m = ex.getMessage();
        // (1) expected-token hint
        assertThat(m).containsIgnoringCase("expecting");
        // (2) caret-annotated snippet
        assertThat(m).contains("^");
        // (3) stable error class / SQLSTATE
        assertThat(m).contains("42601");
        assertThat(m).contains("PARSER-0001");
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.PARSER_SYNTAX_ERROR);
        // position must still be carried structurally
        assertThat(ex.getLine()).isEqualTo(1);
    }

    @Test
    void recoveryParseSyntaxErrorHasExpectedTokenCaretAndSqlstate() {
        ParseResult result = parser.parseWithRecovery("SELECT FROM");
        assertThat(result.hasErrors()).isTrue();
        String m = result.errors().get(0).message();
        assertThat(m).containsIgnoringCase("expecting");
        assertThat(m).contains("^");
        assertThat(m).contains("42601");
        assertThat(m).contains("PARSER-0001");
    }

    @Test
    void caretAlignsUnderOffendingColumn() {
        ParseException ex = catchThrowableOfType(
                () -> parser.parse("SELECT FROM"), ParseException.class);
        assertThat(ex).isNotNull();
        // The snippet line and a caret line must both be present and the caret
        // line must contain only leading whitespace before the '^'.
        String m = ex.getMessage();
        assertThat(m).contains("SELECT FROM");
        String caretLine = m.lines()
                .filter(l -> l.contains("^"))
                .findFirst()
                .orElseThrow();
        assertThat(caretLine.replace("^", "").trim()).isEmpty();
    }
}
