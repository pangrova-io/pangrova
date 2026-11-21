package org.pangrova.parser.parse;

import org.pangrova.parser.api.diagnostic.ParseError;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.api.parse.ParseResult;

import java.util.List;

import org.pangrova.parser.api.ast.Query;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the error-recovery parsing mode ({@link AntlrSqlParser#parseWithRecovery(String)}).
 * Verifies that invalid SQL produces partial AST with error markers instead of throwing.
 */
class ErrorRecoveryParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void astLiteralValidationRetainsPositionAndTreeInRecoveryMode() {
        String sql = "SHOW PRIVILEGE MUTATION 'not-a-uuid'";
        assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(ParseException.class);
        var details = parser.parseWithDetails(sql);
        assertThat(details.result().statement()).isNull();
        assertThat(details.result().errors()).singleElement().satisfies(error -> {
            assertThat(error.line()).isEqualTo(1);
            assertThat(error.column()).isEqualTo(24);
            assertThat(error.message()).contains("canonical request UUID");
        });
        assertThat(details.partialTree()).isNotNull();
    }

    @Test
    void mutableDiagnosticTreeCannotChangePortableResultOrAnotherInvocation() {
        for (String sql : List.of("SELECT '😀' AS label", "SELECT FROM WHERE")) {
            var details = parser.parseWithDetails(sql);
            var independent = parser.parseWithDetails(sql);
            assertThat(details.partialTree()).isNotSameAs(independent.partialTree());
            assertThat(details.result()).isEqualTo(independent.result());
            var tree = (org.antlr.v4.runtime.ParserRuleContext) details.partialTree();
            assertThat(tree.children).isNotEmpty();
            tree.children.clear();
            assertThat(details.partialTree().getChildCount()).isZero();
            assertThat(independent.partialTree().getChildCount()).isPositive();
            assertThat(details.result()).isEqualTo(independent.result());
            assertThat(parser.parseWithRecovery(sql)).isEqualTo(independent.result());
        }
    }

    @Test
    void validSqlProducesSuccessResult() {
        var details = parser.parseWithDetails("SELECT 1");
        ParseResult result = details.result();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.errors()).isEmpty();
        assertThat(result.statement()).isNotNull();
        assertThat(result.statement()).isInstanceOf(Query.class);
        assertThat(details.partialTree()).isNotNull();
    }

    @Test
    void invalidSqlProducesErrorMarkers() {
        var details = parser.parseWithDetails("SELECT FROM WHERE");
        ParseResult result = details.result();

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
        assertThat(details.partialTree()).isNotNull();

        ParseError firstError = result.errors().get(0);
        assertThat(firstError.line()).isGreaterThan(0);
        assertThat(firstError.column()).isGreaterThanOrEqualTo(0);
        assertThat(firstError.message()).isNotBlank();
    }

    @Test
    void incompleteSelectProducesErrorWithPartialTree() {
        var details = parser.parseWithDetails("SELECT a, b FROM");
        ParseResult result = details.result();

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
        assertThat(details.partialTree()).isNotNull();

        // Error should indicate the issue is near the end
        ParseError error = result.errors().get(0);
        assertThat(error.message()).isNotBlank();
    }

    @Test
    void multipleSyntaxErrorsCollected() {
        // Severely malformed SQL should produce multiple errors
        var details = parser.parseWithDetails("SELECT ,,, FROM ### WHERE !!!");
        ParseResult result = details.result();

        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors().size()).isGreaterThanOrEqualTo(1);
        assertThat(details.partialTree()).isNotNull();
    }

    @Test
    void errorMarkersContainLineAndColumn() {
        var details = parser.parseWithDetails("SELECT\nFROM\nWHERE");
        ParseResult result = details.result();

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
        var details = parser.parseWithDetails(sql);
        ParseResult result = details.result();

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
        var details = parser.parseWithDetails("");
        ParseResult result = details.result();

        // Empty input should produce an error (no valid statement)
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).isNotEmpty();
    }

    @Test
    void partialStatementMayBeBuiltFromRecoveredTree() {
        // A query with a minor error at the end may still produce a partial statement
        var details = parser.parseWithDetails("SELECT a FROM t WHERE");
        ParseResult result = details.result();

        assertThat(result.hasErrors()).isTrue();
        assertThat(details.partialTree()).isNotNull();
        // The partial tree is always available for IDE analysis
        assertThat(details.partialTree().getChildCount()).isGreaterThan(0);
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
