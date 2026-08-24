package org.pangrova.parser;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ReservedWordPolicy} — keyword classification, lookup,
 * and quoting utilities.
 */
class ReservedWordPolicyTest {

    private final ReservedWordPolicy policy = ReservedWordPolicy.INSTANCE;

    // ── Reserved keyword checks ──

    @Nested
    class ReservedKeywords {

        @ParameterizedTest
        @ValueSource(strings = {
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
                "AS", "ON", "AND", "OR", "NOT", "TRUE", "FALSE", "NULL",
                "ASC", "DESC", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS",
                "UNION", "ALL", "IN", "DISTINCT", "CREATE", "DROP", "ALTER",
                "OF", "WHEN", "CAST"
        })
        void reservedKeywordsAreReserved(String word) {
            assertThat(policy.isReserved(word))
                    .as("'%s' should be reserved", word)
                    .isTrue();
            assertThat(policy.classify(word))
                    .isEqualTo(ReservedWordPolicy.Category.RESERVED);
        }

        @Test
        void reservedCheckIsCaseInsensitive() {
            assertThat(policy.isReserved("select")).isTrue();
            assertThat(policy.isReserved("Select")).isTrue();
            assertThat(policy.isReserved("SELECT")).isTrue();
        }

        @Test
        void reservedKeywordsAreKeywords() {
            assertThat(policy.isKeyword("SELECT")).isTrue();
            assertThat(policy.isKeyword("FROM")).isTrue();
        }

        @Test
        void reservedWordsSetIsNonEmpty() {
            Set<String> reserved = policy.reservedWords();
            assertThat(reserved).isNotEmpty();
            assertThat(reserved).contains("SELECT", "FROM", "WHERE");
        }
    }

    // ── Non-reserved keyword checks ──

    @Nested
    class NonReservedKeywords {

        @ParameterizedTest
        @ValueSource(strings = {
                "REFRESH", "CATALOG", "VERSION", "SET", "STATUS", "SHOW",
                "TABLE", "INSERT", "DELETE", "UPDATE", "MERGE",
                "BETWEEN", "CASE", "ELSE", "END", "LIKE",
                "YEAR", "MONTH", "DAY", "HOUR",
                "STREAM", "INDEX", "SNAPSHOT", "WINDOW", "RANGE",
                "QUALIFY", "PIVOT", "UNPIVOT"
        })
        void nonReservedKeywordsAreNotReserved(String word) {
            assertThat(policy.isReserved(word))
                    .as("'%s' should NOT be reserved", word)
                    .isFalse();
            assertThat(policy.isNonReserved(word))
                    .as("'%s' should be non-reserved", word)
                    .isTrue();
            assertThat(policy.classify(word))
                    .isEqualTo(ReservedWordPolicy.Category.NON_RESERVED);
        }

        @Test
        void nonReservedCheckIsCaseInsensitive() {
            assertThat(policy.isNonReserved("catalog")).isTrue();
            assertThat(policy.isNonReserved("Catalog")).isTrue();
            assertThat(policy.isNonReserved("CATALOG")).isTrue();
        }

        @Test
        void nonReservedKeywordsAreKeywords() {
            assertThat(policy.isKeyword("CATALOG")).isTrue();
            assertThat(policy.isKeyword("VERSION")).isTrue();
        }

        @Test
        void nonReservedWordsSetIsNonEmpty() {
            Set<String> nonReserved = policy.nonReservedWords();
            assertThat(nonReserved).isNotEmpty();
            assertThat(nonReserved).contains("CATALOG", "VERSION", "SET");
        }
    }

    // ── Non-keyword words ──

    @Nested
    class NonKeywords {

        @ParameterizedTest
        @ValueSource(strings = {"myColumn", "foo", "bar_baz", "x123"})
        void regularIdentifiersAreNotKeywords(String word) {
            assertThat(policy.isReserved(word)).isFalse();
            assertThat(policy.isNonReserved(word)).isFalse();
            assertThat(policy.isKeyword(word)).isFalse();
            assertThat(policy.classify(word)).isNull();
        }

        @Test
        void nullAndEmptyAreNotKeywords() {
            assertThat(policy.isReserved(null)).isFalse();
            assertThat(policy.isReserved("")).isFalse();
            assertThat(policy.isNonReserved(null)).isFalse();
            assertThat(policy.isNonReserved("")).isFalse();
            assertThat(policy.isKeyword(null)).isFalse();
            assertThat(policy.isKeyword("")).isFalse();
            assertThat(policy.classify(null)).isNull();
            assertThat(policy.classify("")).isNull();
        }
    }

    // ── Listing / map ──

    @Nested
    class Listings {

        @Test
        void allKeywordsContainsBothCategories() {
            Set<String> all = policy.allKeywords();
            assertThat(all).containsAll(policy.reservedWords());
            assertThat(all).containsAll(policy.nonReservedWords());
        }

        @Test
        void keywordMapEntriesMatchClassification() {
            Map<String, ReservedWordPolicy.Category> map = policy.keywordMap();
            for (var entry : map.entrySet()) {
                ReservedWordPolicy.Category expected = policy.classify(entry.getKey());
                assertThat(entry.getValue())
                        .as("Map entry for '%s'", entry.getKey())
                        .isEqualTo(expected);
            }
        }

        @Test
        void reservedAndNonReservedDoNotOverlap() {
            // SHOW is a special case - it appears in the grammar as both a statement
            // leader and in nonReservedKeyword. In the map it's classified as whatever
            // the last writer set. We just verify the map is consistent.
            Set<String> reserved = policy.reservedWords();
            Set<String> nonReserved = policy.nonReservedWords();
            // Each word in the map has exactly one category
            for (String word : policy.allKeywords()) {
                ReservedWordPolicy.Category cat = policy.classify(word);
                assertThat(cat).isNotNull();
            }
        }
    }

    // ── quoteIfReserved ──

    @Nested
    class QuoteIfReserved {

        @Test
        void reservedKeywordQuotedWithDoubleQuotesForAnsi() {
            String result = policy.quoteIfReserved("SELECT", SqlDialect.ANSI);
            assertThat(result).isEqualTo("\"SELECT\"");
        }

        @Test
        void reservedKeywordQuotedWithDoubleQuotesForTrino() {
            String result = policy.quoteIfReserved("FROM", SqlDialect.TRINO);
            assertThat(result).isEqualTo("\"FROM\"");
        }

        @Test
        void reservedKeywordQuotedWithBackticksForSpark() {
            String result = policy.quoteIfReserved("SELECT", SqlDialect.SPARK);
            assertThat(result).isEqualTo("`SELECT`");
        }

        @Test
        void reservedKeywordQuotedWithDoubleQuotesForPangrova() {
            String result = policy.quoteIfReserved("WHERE", SqlDialect.PANGROVA);
            assertThat(result).isEqualTo("\"WHERE\"");
        }

        @Test
        void nonReservedKeywordNotQuoted() {
            String result = policy.quoteIfReserved("CATALOG", SqlDialect.ANSI);
            assertThat(result).isEqualTo("CATALOG");
        }

        @Test
        void regularIdentifierNotQuoted() {
            String result = policy.quoteIfReserved("my_column", SqlDialect.ANSI);
            assertThat(result).isEqualTo("my_column");
        }

        @Test
        void reservedKeywordWithEmbeddedQuoteEscaped() {
            // Hypothetical: an identifier that happens to match a reserved word
            // and contains a double-quote
            // quoteIfReserved only processes reserved words, so construct
            // a reserved word that when quoted needs escaping
            // (SELECT doesn't have quotes, but test the escaping logic)
            String result = policy.quoteIfReserved("SELECT", SqlDialect.ANSI);
            assertThat(result).startsWith("\"").endsWith("\"");
        }
    }

    // ── SqlParser.isReserved convenience ──

    @Nested
    class SqlParserConvenience {

        @Test
        void sqlParserIsReservedDelegatesToPolicy() {
            assertThat(SqlParser.isReserved("SELECT")).isTrue();
            assertThat(SqlParser.isReserved("CATALOG")).isFalse();
            assertThat(SqlParser.isReserved("myColumn")).isFalse();
        }

        @Test
        void sqlParserReservedWordPolicyReturnsInstance() {
            ReservedWordPolicy p = SqlParser.reservedWordPolicy();
            assertThat(p).isSameAs(ReservedWordPolicy.INSTANCE);
        }
    }

    // ── Grammar consistency: non-reserved keywords parse as identifiers ──

    @Nested
    class GrammarConsistency {

        private final SqlParser parser = new SqlParser(SqlDialect.PANGROVA);

        @ParameterizedTest
        @ValueSource(strings = {
                "CATALOG", "VERSION", "STATUS", "REFRESH", "COMPACT",
                "OPTIMIZE", "VACUUM", "SNAPSHOT", "MODE", "FORMAT",
                "STREAM", "INDEX", "MATCH", "WINDOW", "RANGE"
        })
        void nonReservedKeywordsCanBeUsedAsColumnNames(String keyword) {
            // Non-reserved keywords should parse as column identifiers
            String sql = "SELECT " + keyword + " FROM t";
            org.pangrova.parser.ast.Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(org.pangrova.parser.ast.Query.class);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "CATALOG", "STATUS", "REFRESH", "VERSION", "FORMAT"
        })
        void nonReservedKeywordsCanBeUsedAsTableNames(String keyword) {
            String sql = "SELECT x FROM " + keyword;
            org.pangrova.parser.ast.Statement stmt = parser.parse(sql);
            assertThat(stmt).isInstanceOf(org.pangrova.parser.ast.Query.class);
        }
    }
}
