package org.pangrova.testing.e2e.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.runner.QueryError;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for error handling correctness.
 *
 * <p>All error scenarios are triggered through the real pipeline:
 * TestCluster HTTP → SqlParser (ANTLR4) → error propagation.
 *
 * <p>Requirements validated: 12.1
 */
// Feature: e2e-sql-verification, Property 24: Syntax error location accuracy
@DisplayName("Error Handling - Property Tests (P24)")
class ErrorPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    // ---------------------------------------------------------------
    // Property 24: Syntax error location accuracy
    // **Validates: Requirements 12.1**
    // ---------------------------------------------------------------

    // Feature: e2e-sql-verification, Property 24: Syntax error location accuracy
    @Property(tries = 10)
    void syntaxErrorLocationAccuracy(
            @ForAll("malformedSqlStatements") MalformedSql malformed) {
        QueryError error = sqlRunner.executeExpectingError(malformed.sql());

        assertThat(error.errorType())
                .as("Error type for malformed SQL: %s", malformed.sql())
                .isEqualTo("SYNTAX");
        assertThat(error.message())
                .as("Error message should not be empty for: %s", malformed.sql())
                .isNotEmpty();
        assertThat(error.message().toLowerCase())
                .as("Error message should contain position info (line/col/char) for: %s",
                        malformed.sql())
                .satisfiesAnyOf(
                        msg -> assertThat(msg).containsAnyOf("line", "position", "at "),
                        msg -> assertThat(msg).containsPattern("\\d+:\\d+"),
                        msg -> assertThat(msg).containsAnyOf("column", "offset", "char")
                );
    }

    // ===============================================================
    // Provider
    // ===============================================================

    @Provide
    Arbitrary<MalformedSql> malformedSqlStatements() {
        // Note: missingFromClause() excluded — "SELECT * WHERE x = 1" is syntactically valid
        // in Calcite (parser accepts it); the error is semantic (validator rejects * without FROM).
        // Note: trailingGarbage() excluded — "SELECT 1 FROM t GARBAGE" is parsed by Calcite
        // (GARBAGE treated as table alias); the error is semantic (validator rejects it).
        return Arbitraries.oneOf(
                invalidKeyword(),
                unclosedParenthesis(),
                duplicateStatements(),
                invalidToken(),
                missingExpression(),
                unclosedStringLiteral()
        );
    }

    // ===============================================================
    // Generator strategies — each produces SQL with a known syntax error
    // ===============================================================

    /** "SELECT * WHERE x = 1" — missing FROM clause. */
    private static Arbitrary<MalformedSql> missingFromClause() {
        return Arbitraries.of("x", "id", "col1", "name").flatMap(col ->
                Arbitraries.integers().between(1, 1000).map(val ->
                        new MalformedSql(
                                "SELECT * WHERE " + col + " = " + val,
                                "missing FROM")));
    }

    /** "SELEC * FROM t" — misspelled keyword. */
    private static Arbitrary<MalformedSql> invalidKeyword() {
        return Arbitraries.of(
                "SELEC", "SLECT", "SELET", "FORM", "WHER", "ODER"
        ).flatMap(bad -> Arbitraries.of("t", "my_table", "orders").map(table ->
                new MalformedSql(
                        bad + " * FROM " + table,
                        "invalid keyword: " + bad)));
    }

    /** "SELECT (1 + 2 FROM t" — unclosed parenthesis. */
    private static Arbitrary<MalformedSql> unclosedParenthesis() {
        return Arbitraries.of("t", "my_table", "orders").flatMap(table ->
                Arbitraries.integers().between(1, 100).map(val ->
                        new MalformedSql(
                                "SELECT (1 + " + val + " FROM " + table,
                                "unclosed parenthesis")));
    }

    /** "SELECT 1 SELECT 2" — two statements without separator. */
    private static Arbitrary<MalformedSql> duplicateStatements() {
        return Arbitraries.integers().between(1, 100).flatMap(a ->
                Arbitraries.integers().between(1, 100).map(b ->
                        new MalformedSql(
                                "SELECT " + a + " SELECT " + b,
                                "duplicate SELECT")));
    }

    /** "SELECT @#$ FROM t" — invalid token characters. */
    private static Arbitrary<MalformedSql> invalidToken() {
        return Arbitraries.of("@#$", "!!!", "~~~", "^^^", "&&&").flatMap(token ->
                Arbitraries.of("t", "my_table", "orders").map(table ->
                        new MalformedSql(
                                "SELECT " + token + " FROM " + table,
                                "invalid token: " + token)));
    }

    /** "SELECT 1 FROM t GARBAGE" — trailing garbage after valid statement. */
    private static Arbitrary<MalformedSql> trailingGarbage() {
        return Arbitraries.of("GARBAGE", "BLAH", "XYZ", "FOOBAR").flatMap(garbage ->
                Arbitraries.of("t", "my_table", "orders").map(table ->
                        new MalformedSql(
                                "SELECT 1 FROM " + table + " " + garbage,
                                "trailing garbage: " + garbage)));
    }

    /** "SELECT FROM t" — missing expression after SELECT. */
    private static Arbitrary<MalformedSql> missingExpression() {
        return Arbitraries.of("t", "my_table", "orders").map(table ->
                new MalformedSql(
                        "SELECT FROM " + table,
                        "missing expression after SELECT"));
    }

    /** "SELECT 'unclosed FROM t" — unclosed string literal. */
    private static Arbitrary<MalformedSql> unclosedStringLiteral() {
        return Arbitraries.of("hello", "world", "test", "abc").flatMap(str ->
                Arbitraries.of("t", "my_table", "orders").map(table ->
                        new MalformedSql(
                                "SELECT '" + str + " FROM " + table,
                                "unclosed string literal")));
    }

    // ===============================================================
    // Helper record
    // ===============================================================

    /** Pairs a malformed SQL string with a human-readable description. */
    record MalformedSql(String sql, String description) {

        @Override
        public String toString() {
            return description + " → " + sql;
        }
    }
}
