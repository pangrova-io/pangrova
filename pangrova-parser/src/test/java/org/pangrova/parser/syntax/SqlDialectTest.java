package org.pangrova.parser.syntax;

import org.pangrova.parser.api.enums.IdentifierCaseSensitivity;
import org.pangrova.parser.api.enums.IdentifierQuotingStyle;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.api.enums.SqlDialect;
import org.pangrova.parser.api.ast.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SQL dialect configuration: identifier quoting and case sensitivity.
 * Covers ANSI, TRINO, SPARK, and PANGROVA (default) dialects.
 */
class SqlDialectTest {

    // ── SqlDialect enum unit tests ──

    @Test
    void ansiDialectProperties() {
        assertThat(SqlDialect.ANSI.quotingStyle()).isEqualTo(IdentifierQuotingStyle.DOUBLE_QUOTES);
        assertThat(SqlDialect.ANSI.caseSensitivity()).isEqualTo(IdentifierCaseSensitivity.UPPER);
        assertThat(SqlDialect.ANSI.allowsDoubleQuotes()).isTrue();
        assertThat(SqlDialect.ANSI.allowsBackticks()).isFalse();
    }

    @Test
    void trinoDialectProperties() {
        assertThat(SqlDialect.TRINO.quotingStyle()).isEqualTo(IdentifierQuotingStyle.DOUBLE_QUOTES);
        assertThat(SqlDialect.TRINO.caseSensitivity()).isEqualTo(IdentifierCaseSensitivity.LOWER);
        assertThat(SqlDialect.TRINO.allowsDoubleQuotes()).isTrue();
        assertThat(SqlDialect.TRINO.allowsBackticks()).isFalse();
    }

    @Test
    void sparkDialectProperties() {
        assertThat(SqlDialect.SPARK.quotingStyle()).isEqualTo(IdentifierQuotingStyle.BACKTICKS);
        assertThat(SqlDialect.SPARK.caseSensitivity()).isEqualTo(IdentifierCaseSensitivity.PRESERVE);
        assertThat(SqlDialect.SPARK.allowsDoubleQuotes()).isFalse();
        assertThat(SqlDialect.SPARK.allowsBackticks()).isTrue();
    }

    @Test
    void pangrovaDialectProperties() {
        assertThat(SqlDialect.PANGROVA.quotingStyle()).isEqualTo(IdentifierQuotingStyle.BOTH);
        assertThat(SqlDialect.PANGROVA.caseSensitivity()).isEqualTo(IdentifierCaseSensitivity.PRESERVE);
        assertThat(SqlDialect.PANGROVA.allowsDoubleQuotes()).isTrue();
        assertThat(SqlDialect.PANGROVA.allowsBackticks()).isTrue();
    }

    @Test
    void foldIdentifier_upper() {
        assertThat(SqlDialect.ANSI.foldIdentifier("myCol")).isEqualTo("MYCOL");
    }

    @Test
    void foldIdentifier_lower() {
        assertThat(SqlDialect.TRINO.foldIdentifier("MyCol")).isEqualTo("mycol");
    }

    @Test
    void foldIdentifier_preserve() {
        assertThat(SqlDialect.PANGROVA.foldIdentifier("MyCol")).isEqualTo("MyCol");
    }

    // ── Default parser (PANGROVA dialect) — backward compatibility ──

    @Nested
    class DefaultDialect {
        private final AntlrSqlParser parser = new AntlrSqlParser();

        @Test
        void defaultDialectIsPangrova() {
            assertThat(parser.dialect()).isEqualTo(SqlDialect.PANGROVA);
        }

        @Test
        void parseSelectWithDoubleQuotedIdentifier() {
            Statement stmt = parser.parse("SELECT \"MyColumn\" FROM t");
            assertThat(stmt).isInstanceOf(Query.class);
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            // Double-quoted: case preserved exactly
            assertThat(id.parts()).containsExactly("MyColumn");
        }

        @Test
        void parseSelectWithBacktickIdentifier() {
            Statement stmt = parser.parse("SELECT `MyColumn` FROM t");
            assertThat(stmt).isInstanceOf(Query.class);
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            // Backtick-quoted: case preserved exactly
            assertThat(id.parts()).containsExactly("MyColumn");
        }

        @Test
        void parseSelectUnquotedPreservesCase() {
            Statement stmt = parser.parse("SELECT MyColumn FROM t");
            assertThat(stmt).isInstanceOf(Query.class);
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            // PANGROVA default: case preserved
            assertThat(id.parts()).containsExactly("MyColumn");
        }
    }

    // ── ANSI dialect ──

    @Nested
    class AnsiDialect {
        private final AntlrSqlParser parser = new AntlrSqlParser(SqlDialect.ANSI);

        @Test
        void unquotedIdentifierFoldsToUpper() {
            Statement stmt = parser.parse("SELECT myCol FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("MYCOL");
        }

        @Test
        void doubleQuotedIdentifierPreservesCase() {
            Statement stmt = parser.parse("SELECT \"myCol\" FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("myCol");
        }

        @Test
        void backtickIdentifierRejected() {
            assertThatThrownBy(() -> parser.parse("SELECT `myCol` FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Backtick-quoted identifiers are not allowed in ANSI dialect");
        }

        @Test
        void qualifiedNameFoldsToUpper() {
            Statement stmt = parser.parse("SELECT mySchema.myTable.myCol FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("MYSCHEMA", "MYTABLE", "MYCOL");
        }

        @Test
        void tableNameInFromFoldsToUpper() {
            Statement stmt = parser.parse("SELECT a FROM myTable");
            Query q = (Query) stmt;
            TableRef table = q.from().get(0);
            assertThat(table.name().parts()).containsExactly("MYTABLE");
        }

        @Test
        void doubleQuotedEscapeHandled() {
            Statement stmt = parser.parse("SELECT \"my\"\"col\" FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            // Escaped double-quote inside: my"col
            assertThat(id.parts()).containsExactly("my\"col");
        }

        @Test
        void keywordsAsFoldedIdentifiers() {
            // Non-reserved keywords used as identifiers should also be folded
            Statement stmt = parser.parse("SELECT status FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("STATUS");
        }
    }

    // ── TRINO dialect ──

    @Nested
    class TrinoDialect {
        private final AntlrSqlParser parser = new AntlrSqlParser(SqlDialect.TRINO);

        @Test
        void unquotedIdentifierFoldsToLower() {
            Statement stmt = parser.parse("SELECT MyCol FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("mycol");
        }

        @Test
        void doubleQuotedPreservesCase() {
            Statement stmt = parser.parse("SELECT \"MyCol\" FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("MyCol");
        }

        @Test
        void backtickRejected() {
            assertThatThrownBy(() -> parser.parse("SELECT `x` FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Backtick-quoted identifiers are not allowed in TRINO dialect");
        }

        @Test
        void tableAndColumnFoldToLower() {
            Statement stmt = parser.parse("SELECT Col1 FROM MyTable");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier colId = (Identifier) item.expression();
            assertThat(colId.parts()).containsExactly("col1");
            assertThat(q.from().get(0).name().parts()).containsExactly("mytable");
        }
    }

    // ── SPARK dialect ──

    @Nested
    class SparkDialect {
        private final AntlrSqlParser parser = new AntlrSqlParser(SqlDialect.SPARK);

        @Test
        void backtickIdentifierAccepted() {
            Statement stmt = parser.parse("SELECT `MyCol` FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("MyCol");
        }

        @Test
        void doubleQuotedIdentifierRejected() {
            assertThatThrownBy(() -> parser.parse("SELECT \"myCol\" FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Double-quoted identifiers are not allowed in SPARK dialect");
        }

        @Test
        void unquotedPreservesCase() {
            Statement stmt = parser.parse("SELECT MyCol FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("MyCol");
        }

        @Test
        void backtickEscapeHandled() {
            Statement stmt = parser.parse("SELECT `my``col` FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            // Escaped backtick: my`col
            assertThat(id.parts()).containsExactly("my`col");
        }

        @Test
        void backtickQualifiedName() {
            Statement stmt = parser.parse("SELECT `myDb`.`myTable`.`myCol` FROM t");
            Query q = (Query) stmt;
            ExpressionItem item = (ExpressionItem) q.selectItems().get(0);
            Identifier id = (Identifier) item.expression();
            assertThat(id.parts()).containsExactly("myDb", "myTable", "myCol");
        }
    }

    // ── Cross-dialect: same SQL, different results ──

    @Nested
    class CrossDialect {
        @Test
        void sameUnquotedIdentifierDifferentFolding() {
            String sql = "SELECT myCol FROM t";

            AntlrSqlParser ansiParser = new AntlrSqlParser(SqlDialect.ANSI);
            AntlrSqlParser trinoParser = new AntlrSqlParser(SqlDialect.TRINO);
            AntlrSqlParser sparkParser = new AntlrSqlParser(SqlDialect.SPARK);

            Query ansiQ = (Query) ansiParser.parse(sql);
            Query trinoQ = (Query) trinoParser.parse(sql);
            Query sparkQ = (Query) sparkParser.parse(sql);

            String ansiCol = ((Identifier) ((ExpressionItem) ansiQ.selectItems().get(0)).expression())
                    .parts().get(0);
            String trinoCol = ((Identifier) ((ExpressionItem) trinoQ.selectItems().get(0)).expression())
                    .parts().get(0);
            String sparkCol = ((Identifier) ((ExpressionItem) sparkQ.selectItems().get(0)).expression())
                    .parts().get(0);

            assertThat(ansiCol).isEqualTo("MYCOL");
            assertThat(trinoCol).isEqualTo("mycol");
            assertThat(sparkCol).isEqualTo("myCol");
        }

        @Test
        void pangrovaDialectAcceptsBothQuotingStyles() {
            AntlrSqlParser parser = new AntlrSqlParser(SqlDialect.PANGROVA);

            Statement dq = parser.parse("SELECT \"col1\" FROM t");
            Statement bt = parser.parse("SELECT `col2` FROM t");

            assertThat(dq).isInstanceOf(Query.class);
            assertThat(bt).isInstanceOf(Query.class);

            String dqCol = ((Identifier) ((ExpressionItem) ((Query) dq).selectItems().get(0)).expression())
                    .parts().get(0);
            String btCol = ((Identifier) ((ExpressionItem) ((Query) bt).selectItems().get(0)).expression())
                    .parts().get(0);

            assertThat(dqCol).isEqualTo("col1");
            assertThat(btCol).isEqualTo("col2");
        }
    }

    // ── normalizeQuoting ──

    @Nested
    class NormalizeQuoting {

        @Test
        void backtickToDoubleQuoteForAnsi() {
            String result = SqlDialect.ANSI.normalizeQuoting("`myCol`");
            assertThat(result).isEqualTo("\"myCol\"");
        }

        @Test
        void backtickToDoubleQuoteForTrino() {
            String result = SqlDialect.TRINO.normalizeQuoting("`myCol`");
            assertThat(result).isEqualTo("\"myCol\"");
        }

        @Test
        void doubleQuoteToBacktickForSpark() {
            String result = SqlDialect.SPARK.normalizeQuoting("\"myCol\"");
            assertThat(result).isEqualTo("`myCol`");
        }

        @Test
        void backtickUnchangedForSpark() {
            String result = SqlDialect.SPARK.normalizeQuoting("`myCol`");
            assertThat(result).isEqualTo("`myCol`");
        }

        @Test
        void doubleQuoteUnchangedForAnsi() {
            String result = SqlDialect.ANSI.normalizeQuoting("\"myCol\"");
            assertThat(result).isEqualTo("\"myCol\"");
        }

        @Test
        void pangrovaKeepsBothStyles() {
            assertThat(SqlDialect.PANGROVA.normalizeQuoting("`myCol`")).isEqualTo("`myCol`");
            assertThat(SqlDialect.PANGROVA.normalizeQuoting("\"myCol\"")).isEqualTo("\"myCol\"");
        }

        @Test
        void backtickEscapedQuoteConvertedToDoubleQuote() {
            // `my``col` -> "my`col"
            String result = SqlDialect.ANSI.normalizeQuoting("`my``col`");
            assertThat(result).isEqualTo("\"my`col\"");
        }

        @Test
        void doubleQuoteEscapedConvertedToBacktick() {
            // "my""col" -> `my"col`
            String result = SqlDialect.SPARK.normalizeQuoting("\"my\"\"col\"");
            assertThat(result).isEqualTo("`my\"col`");
        }

        @Test
        void unquotedIdentifierUnchanged() {
            assertThat(SqlDialect.ANSI.normalizeQuoting("myCol")).isEqualTo("myCol");
        }

        @Test
        void nullReturnedAsIs() {
            assertThat(SqlDialect.ANSI.normalizeQuoting(null)).isNull();
        }

        @Test
        void singleCharReturnedAsIs() {
            assertThat(SqlDialect.ANSI.normalizeQuoting("x")).isEqualTo("x");
        }
    }

    // ── AntlrSqlParser constructor validation ──

    @Test
    void nullDialectThrows() {
        assertThatThrownBy(() -> new AntlrSqlParser(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dialect must not be null");
    }

    @Test
    void dialectConstructorPassesThroughLimits() {
        AntlrSqlParser parser = new AntlrSqlParser(SqlDialect.TRINO);
        assertThat(parser.dialect()).isEqualTo(SqlDialect.TRINO);
        // Should still work with default limits
        Statement stmt = parser.parse("SELECT 1");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    // ── Cache isolation by dialect ──

    @Test
    void cacheKeyIncludesDialectImplicitly() {
        // Different dialects produce different ASTs for the same SQL.
        // Since they are different AntlrSqlParser instances with separate caches,
        // there is no cross-contamination.
        AntlrSqlParser ansi = new AntlrSqlParser(SqlDialect.ANSI);
        AntlrSqlParser trino = new AntlrSqlParser(SqlDialect.TRINO);

        ansi.parse("SELECT col FROM t");
        trino.parse("SELECT col FROM t");

        Query ansiResult = (Query) ansi.parse("SELECT col FROM t");
        Query trinoResult = (Query) trino.parse("SELECT col FROM t");

        String ansiCol = ((Identifier) ((ExpressionItem) ansiResult.selectItems().get(0)).expression())
                .parts().get(0);
        String trinoCol = ((Identifier) ((ExpressionItem) trinoResult.selectItems().get(0)).expression())
                .parts().get(0);

        assertThat(ansiCol).isEqualTo("COL");
        assertThat(trinoCol).isEqualTo("col");
    }
}
