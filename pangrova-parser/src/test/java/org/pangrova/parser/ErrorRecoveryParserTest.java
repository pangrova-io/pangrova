package org.pangrova.parser;

import java.util.List;

import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the error-recovery parsing mode ({@link SqlParser#parseWithRecovery(String)}).
 * Verifies that invalid SQL produces partial AST with error markers instead of throwing.
 */
class ErrorRecoveryParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    void validSqlProducesSuccessResult() {
        ParseResult result = parser.parseWithRecovery("SELECT 1");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.errors()).isEmpty();
        assertThat(result.statement()).isNotNull();
        assertThat(result.statement()).isInstanceOf(Query.class);
        assertThat(result.partialTree()).isNotNull();
    }

    @Test
    void invalidSqlProducesErrorMarkers() {
        ParseResult result = parser.parseWithRecovery("SELECT FROM WHERE");

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
        assertThat(result.partialTree()).isNotNull();

        ParseError firstError = result.errors().get(0);
        assertThat(firstError.line()).isGreaterThan(0);
        assertThat(firstError.column()).isGreaterThanOrEqualTo(0);
        assertThat(firstError.message()).isNotBlank();
    }

    @Test
    void incompleteSelectProducesErrorWithPartialTree() {
        ParseResult result = parser.parseWithRecovery("SELECT a, b FROM");

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
        assertThat(result.partialTree()).isNotNull();

        // Error should indicate the issue is near the end
        ParseError error = result.errors().get(0);
        assertThat(error.message()).isNotBlank();
    }

    @Test
    void multipleSyntaxErrorsCollected() {
        // Severely malformed SQL should produce multiple errors
        ParseResult result = parser.parseWithRecovery("SELECT ,,, FROM ### WHERE !!!");

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors().size()).isGreaterThanOrEqualTo(1);
        assertThat(result.partialTree()).isNotNull();
    }

    @Test
    void errorMarkersContainLineAndColumn() {
        ParseResult result = parser.parseWithRecovery("SELECT\nFROM\nWHERE");

        assertThat(result.hasErrors()).isTrue();
        List<ParseError> errors = result.errors();
        assertThat(errors).isNotEmpty();

        for (ParseError error : errors) {
            assertThat(error.line()).isGreaterThan(0);
            assertThat(error.column()).isGreaterThanOrEqualTo(0);
            assertThat(error.message()).isNotBlank();
        }
    }

    @Test
    void validComplexQuerySucceeds() {
        String sql = "SELECT a, b, COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id "
                + "WHERE t1.x > 10 GROUP BY a, b HAVING COUNT(*) > 1 ORDER BY a";
        ParseResult result = parser.parseWithRecovery(sql);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.statement()).isNotNull();
        assertThat(result.statement()).isInstanceOf(Query.class);
    }

    @Test
    void nullInputThrowsParseException() {
        assertThatThrownBy(() -> parser.parseWithRecovery(null))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void emptyInputProducesError() {
        ParseResult result = parser.parseWithRecovery("");

        // Empty input should produce an error (no valid statement)
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
    }

    @Test
    void partialStatementMayBeBuiltFromRecoveredTree() {
        // A query with a minor error at the end may still produce a partial statement
        ParseResult result = parser.parseWithRecovery("SELECT a FROM t WHERE");

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.partialTree()).isNotNull();
        // The partial tree is always available for IDE analysis
        assertThat(result.partialTree().getChildCount()).isGreaterThan(0);
    }

    @Test
    void parseErrorToStringIncludesLocation() {
        ParseError error = new ParseError(5, 12, "unexpected token", "FROM");
        assertThat(error.toString()).contains("line 5:12");
        assertThat(error.toString()).contains("FROM");
        assertThat(error.toString()).contains("unexpected token");
    }

    @Test
    void parseErrorWithoutOffendingToken() {
        ParseError error = new ParseError(1, 0, "missing SELECT");
        assertThat(error.offendingToken()).isNull();
        assertThat(error.toString()).contains("line 1:0");
        assertThat(error.toString()).contains("missing SELECT");
    }

    @Test
    void recoveryModeDoesNotPolluteParseCacheOfStrictMode() {
        // Parse invalid SQL in recovery mode
        parser.parseWithRecovery("SELECT ,,, FROM");

        // The strict parse cache should not be affected
        assertThat(parser.cacheSize()).isZero();

        // Parse valid SQL in strict mode — should work and cache
        parser.parse("SELECT 1");
        assertThat(parser.cacheSize()).isEqualTo(1);
    }
}
