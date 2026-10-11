package org.pangrova.parser;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    // ── Happy path ──

    @Test
    void parseSelectIntegerLiteral() {
        Statement stmt = parser.parse("SELECT 1");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void parseSelectStringLiteral() {
        Statement stmt = parser.parse("SELECT 'hello'");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void parseSelectArithmeticExpression() {
        Statement stmt = parser.parse("SELECT 1 + 2");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void valuesDerivedTablePreservesRowsAndDeclaredColumnNames() {
        Query query = (Query) parser.parse(
                "SELECT X FROM (VALUES (1, 2), (3, 4)) AS t(X, Y)");
        TableRef source = query.from().get(0);
        assertThat(source.alias()).contains("t");
        ValuesTable values = source.valuesTable().orElseThrow();
        assertThat(values.rows()).hasSize(2);
        assertThat(values.columnAliases()).containsExactly("X", "Y");
        assertThat(new AstPrinter().print(query)).contains("VALUES (1, 2), (3, 4)");
    }

    @Test
    void valuesDerivedTableRejectsRaggedRows() {
        assertThatThrownBy(() -> parser.parse("SELECT X FROM (VALUES (1, 2), (3)) AS t(X, Y)"))
                .hasMessageContaining("rows must have equal width");
    }

    @Test
    void valuesDerivedTableRejectsWrongColumnAliasCount() {
        assertThatThrownBy(() -> parser.parse("SELECT X FROM (VALUES (1, 2)) AS t(X)"))
                .hasMessageContaining("column alias count");
    }

    // ── Error reporting ──

    @Test
    void invalidSqlThrowsParseExceptionWithPosition() {
        assertThatThrownBy(() -> parser.parse("SELEC 1"))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_SYNTAX_ERROR);
    }

    @Test
    void emptySqlThrowsParseException() {
        assertThatThrownBy(() -> parser.parse(""))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void errorMessagePreservesUserInputKeywords() {
        // Mixed-case SQL with uppercase keywords — error message must preserve user input
        // and not replace SQL keywords like SELECT, FROM, DROP with '...'
        ParseException ex = catchThrowableOfType(
                () -> parser.parse("SELECT * FROM DROP"),
                ParseException.class);
        // The old regex would turn 'DROP' into '...' because it matched all uppercase tokens.
        // The fixed regex only replaces ANTLR internal names (those with underscores).
        assertThat(ex.getMessage()).doesNotContain("'...'");
    }
}
