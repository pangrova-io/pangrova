package org.pangrova.parser.parse;

import org.pangrova.parser.build.antlr.PangrovaSqlLexer;
import org.pangrova.parser.build.antlr.PangrovaSqlParser;
import org.pangrova.parser.build.AstBuilder;

import org.pangrova.parser.api.ast.SourceLocation;
import org.pangrova.parser.build.SourceLocationRegistry;
import org.pangrova.parser.api.ast.Statement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the parser populates SourceLocation correctly for
 * SELECT, INSERT, and CREATE TABLE statements, and that the
 * SourceLocationRegistry is populated after parsing.
 */
class SourceLocationTest {

    /**
     * Parses SQL using AstBuilder directly so we can access the locationRegistry.
     */
    private ParseOutcome parseWithLocation(String sql) {
        PangrovaSqlLexer lexer = new PangrovaSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PangrovaSqlParser parser = new PangrovaSqlParser(tokens);
        parser.removeErrorListeners();
        PangrovaSqlParser.SingleStatementContext tree = parser.singleStatement();
        AstBuilder builder = new AstBuilder(tokens);
        Statement stmt = builder.visitSingleStatement(tree);
        return new ParseOutcome(stmt, builder.locationRegistry());
    }

    private record ParseOutcome(Statement statement, SourceLocationRegistry registry) {}

    @Test
    void borrowedTokenStreamPreservesViewSqlWithSupplementaryCharactersAndComments() {
        String body = "SELECT '😀' /* preserve comment */ AS emoji";
        var outcome = parseWithLocation("CREATE VIEW demo AS " + body);
        var view = (org.pangrova.parser.api.ast.statement.CreateViewStatement) outcome.statement();
        assertThat(view.viewSql()).isEqualTo(body);
    }

    @Test
    void selectStatementHasCorrectLocation() {
        ParseOutcome outcome = parseWithLocation("SELECT 1");
        Statement stmt = outcome.statement();

        Optional<SourceLocation> loc = outcome.registry().lookup(stmt);
        assertThat(loc).isPresent();
        assertThat(loc.get().line()).isEqualTo(1);
        assertThat(loc.get().col()).isEqualTo(1);
        assertThat(loc.get().startTok()).isGreaterThanOrEqualTo(0);
        assertThat(loc.get().endTok()).isGreaterThanOrEqualTo(loc.get().startTok());
    }

    @Test
    void selectStatementOnSecondLineHasCorrectLocation() {
        ParseOutcome outcome = parseWithLocation("\nSELECT * FROM t1");
        Statement stmt = outcome.statement();

        Optional<SourceLocation> loc = outcome.registry().lookup(stmt);
        assertThat(loc).isPresent();
        // SELECT starts on line 2 (after the leading newline)
        assertThat(loc.get().line()).isEqualTo(2);
        assertThat(loc.get().col()).isEqualTo(1);
    }

    @Test
    void insertStatementHasCorrectLocation() {
        ParseOutcome outcome = parseWithLocation("INSERT INTO t1 VALUES (1, 'hello')");
        Statement stmt = outcome.statement();

        Optional<SourceLocation> loc = outcome.registry().lookup(stmt);
        assertThat(loc).isPresent();
        assertThat(loc.get().line()).isEqualTo(1);
        assertThat(loc.get().col()).isEqualTo(1);
        assertThat(loc.get().startTok()).isGreaterThanOrEqualTo(0);
        assertThat(loc.get().endTok()).isGreaterThan(loc.get().startTok());
    }

    @Test
    void createTableStatementHasCorrectLocation() {
        ParseOutcome outcome = parseWithLocation("CREATE TABLE t1 (id INT, name VARCHAR)");
        Statement stmt = outcome.statement();

        Optional<SourceLocation> loc = outcome.registry().lookup(stmt);
        assertThat(loc).isPresent();
        assertThat(loc.get().line()).isEqualTo(1);
        assertThat(loc.get().col()).isEqualTo(1);
        assertThat(loc.get().startTok()).isGreaterThanOrEqualTo(0);
        assertThat(loc.get().endTok()).isGreaterThan(loc.get().startTok());
    }

    @Test
    void createTableWithOffsetHasCorrectColumnPosition() {
        // Leading spaces shift the column position
        ParseOutcome outcome = parseWithLocation("   CREATE TABLE t1 (id INT)");
        Statement stmt = outcome.statement();

        Optional<SourceLocation> loc = outcome.registry().lookup(stmt);
        assertThat(loc).isPresent();
        assertThat(loc.get().line()).isEqualTo(1);
        // col is 1-based: 3 spaces + 1 = column 4
        assertThat(loc.get().col()).isEqualTo(4);
    }

    @Test
    void registryIsPopulatedAfterParsing() {
        ParseOutcome outcome = parseWithLocation("SELECT a, b FROM t1 WHERE a > 1");
        SourceLocationRegistry registry = outcome.registry();

        // The registry should contain at least the top-level statement
        assertThat(registry.size()).isGreaterThan(0);
        // The top-level statement should be registered
        assertThat(registry.lookup(outcome.statement())).isPresent();
    }

    @Test
    void multipleStatementTypesAllRegistered() {
        // Verify each statement type independently populates the registry
        String[] sqls = {
                "SELECT 1",
                "INSERT INTO t1 VALUES (1)",
                "CREATE TABLE t2 (id INT)"
        };

        for (String sql : sqls) {
            ParseOutcome outcome = parseWithLocation(sql);
            assertThat(outcome.registry().size())
                    .as("Registry should be populated for: %s", sql)
                    .isGreaterThan(0);
            assertThat(outcome.registry().lookup(outcome.statement()))
                    .as("Statement should have location for: %s", sql)
                    .isPresent();
        }
    }
}
