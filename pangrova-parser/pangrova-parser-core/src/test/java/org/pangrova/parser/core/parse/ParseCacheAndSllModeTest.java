package org.pangrova.parser.core.parse;

import org.pangrova.parser.api.diagnostic.ParseException;

import org.pangrova.parser.api.ast.Statement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parse cache (adaptive LRU, keyed by whitespace-normalized SQL)
 * and SLL/LL prediction mode fallback with statistics tracking.
 *
 * <p>Literals are intentionally NOT stripped from cache keys: the cached AST
 * embeds concrete literal values, so two SQLs differing only in literals must
 * not share a cache entry. See {@link AntlrSqlParser#normalizeCacheKey} for details.
 */
class ParseCacheAndSllModeTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    // ── Cache hit tests ──

    @Test
    void repeatedParseReturnsSameAstInstance() {
        Statement first = parser.parse("SELECT 1");
        Statement second = parser.parse("SELECT 1");
        assertThat(second).isSameAs(first);
    }

    @Test
    void cacheNormalizesWhitespace() {
        Statement first = parser.parse("SELECT   1");
        Statement second = parser.parse("SELECT 1");
        assertThat(second).isSameAs(first);
    }

    @Test
    void cacheNormalizesLeadingTrailingWhitespace() {
        Statement first = parser.parse("  SELECT 1  ");
        Statement second = parser.parse("SELECT 1");
        assertThat(second).isSameAs(first);
    }

    @Test
    void cacheNormalizesNewlinesAndTabs() {
        Statement first = parser.parse("SELECT\n\t1");
        Statement second = parser.parse("SELECT 1");
        assertThat(second).isSameAs(first);
    }

    @Test
    void cachePreservesNumericLiterals() {
        // Queries differing in numeric literals must NOT share a cache entry,
        // because the cached AST embeds the concrete literal value.
        Statement first = parser.parse("SELECT * FROM t WHERE id = 1");
        Statement second = parser.parse("SELECT * FROM t WHERE id = 2");
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void cachePreservesStringLiterals() {
        // Queries differing in string literals must NOT share a cache entry,
        // because the cached AST embeds the concrete literal value.
        Statement first = parser.parse("SELECT * FROM t WHERE name = 'alice'");
        Statement second = parser.parse("SELECT * FROM t WHERE name = 'bob'");
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void differentStructureProducesDifferentCacheEntries() {
        // Structurally different queries must NOT share a cache entry
        Statement first = parser.parse("SELECT a FROM t");
        Statement second = parser.parse("SELECT b FROM t WHERE x > 1");
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void cacheSizeReflectsStoredEntries() {
        parser.parse("SELECT a FROM t1");
        parser.parse("SELECT b FROM t2");
        parser.parse("SELECT c FROM t3");
        assertThat(parser.cacheSize()).isEqualTo(3);
    }

    @Test
    void invalidateCacheClearsAllEntries() {
        parser.parse("SELECT 1");
        parser.parse("SELECT a FROM t");
        parser.invalidateCache();
        assertThat(parser.cacheSize()).isEqualTo(0);
    }

    @Test
    void negativeCacheSizeThrowsException() {
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new AntlrSqlParser(1_048_576, 256, -1));
    }

    // ── Cache statistics tests ──

    @Test
    void cacheHitRateTracksCorrectly() {
        parser.parse("SELECT 1");       // miss
        parser.parse("SELECT 1");       // hit (same SQL)
        parser.parse("SELECT 1");       // hit (same SQL)
        assertThat(parser.cacheHitCount()).isEqualTo(2);
        assertThat(parser.cacheMissCount()).isEqualTo(1);
        assertThat(parser.cacheHitRate()).isCloseTo(2.0 / 3.0, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void invalidateCacheResetsStatistics() {
        parser.parse("SELECT 1");
        parser.parse("SELECT 2");
        parser.invalidateCache();
        assertThat(parser.cacheHitRate()).isEqualTo(0.0);
        assertThat(parser.cacheHitCount()).isEqualTo(0);
        assertThat(parser.cacheMissCount()).isEqualTo(0);
    }

    // ── SLL/LL fallback tests ──

    @Test
    void simpleQueryParsesViaSllMode() {
        Statement stmt = parser.parse("SELECT a, b FROM t WHERE x > 1");
        assertThat(stmt).isNotNull();
        assertThat(parser.sllSuccessRate()).isEqualTo(1.0);
    }

    @Test
    void complexQueryWithSubqueryParses() {
        Statement stmt = parser.parse(
                "SELECT * FROM t WHERE id IN (SELECT id FROM s WHERE s.val > 10)");
        assertThat(stmt).isNotNull();
    }

    @Test
    void joinQueryParses() {
        Statement stmt = parser.parse(
                "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id WHERE a.x > 1");
        assertThat(stmt).isNotNull();
    }

    @Test
    void invalidSqlStillThrowsParseException() {
        assertThatThrownBy(() -> parser.parse("SELEC INVALID GARBAGE"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void invalidSqlIsNotCached() {
        try {
            parser.parse("SELEC INVALID");
        } catch (ParseException expected) {
            // Syntax error is expected
        }
        assertThat(parser.cacheSize()).isEqualTo(0);
    }

    @Test
    void sllSuccessRateDefaultsToOneWhenNoParsesYet() {
        AntlrSqlParser fresh = new AntlrSqlParser();
        assertThat(fresh.sllSuccessRate()).isEqualTo(1.0);
    }

    // ── Normalization unit tests ──

    @Test
    void normalizeSqlCollapsesTabs() {
        assertThat(AntlrSqlParser.normalizeSql("SELECT\t\t1")).isEqualTo("SELECT 1");
    }

    @Test
    void normalizeSqlTrimsEdges() {
        assertThat(AntlrSqlParser.normalizeSql("  SELECT 1  ")).isEqualTo("SELECT 1");
    }

    @Test
    void normalizeSqlCollapsesNewlines() {
        assertThat(AntlrSqlParser.normalizeSql("SELECT\n\n1")).isEqualTo("SELECT 1");
    }

    @Test
    void normalizeCacheKeyPreservesNumericLiterals() {
        // Literals are preserved so AST identity matches SQL identity.
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT * FROM t WHERE id = 42"))
                .isEqualTo("SELECT * FROM t WHERE id = 42");
    }

    @Test
    void normalizeCacheKeyPreservesDecimalLiterals() {
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT * FROM t WHERE price > 19.99"))
                .isEqualTo("SELECT * FROM t WHERE price > 19.99");
    }

    @Test
    void normalizeCacheKeyPreservesStringLiterals() {
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT * FROM t WHERE name = 'hello'"))
                .isEqualTo("SELECT * FROM t WHERE name = 'hello'");
    }

    @Test
    void normalizeCacheKeyCollapsesWhitespaceButPreservesLiterals() {
        assertThat(AntlrSqlParser.normalizeCacheKey("INSERT INTO t  VALUES (1,\n'a',\t3.14)"))
                .isEqualTo("INSERT INTO t VALUES (1, 'a', 3.14)");
    }

    @Test
    void normalizeCacheKeyPreservesWhitespaceInsideStringLiterals() {
        // F-PARSER-01: whitespace inside single-quoted strings must NOT be collapsed
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT * FROM t WHERE name = 'hello   world'"))
                .isEqualTo("SELECT * FROM t WHERE name = 'hello   world'");
    }

    @Test
    void normalizeCacheKeyPreservesEscapedQuotesInsideLiterals() {
        // Escaped single quotes ('') inside string literals must be handled correctly
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT 'it''s   fine'"))
                .isEqualTo("SELECT 'it''s   fine'");
    }

    @Test
    void normalizeCacheKeyPreservesWhitespaceInsideDoubleQuotedIdentifiers() {
        // Double-quoted identifiers can contain spaces
        assertThat(AntlrSqlParser.normalizeCacheKey("SELECT \"column  name\"  FROM  t"))
                .isEqualTo("SELECT \"column  name\" FROM t");
    }

    @Test
    void normalizeCacheKeyDifferentLiteralWhitespaceDifferentKeys() {
        // Two SQL strings differing only in literal whitespace must produce different keys
        String sql1 = "SELECT * FROM t WHERE name = 'hello world'";
        String sql2 = "SELECT * FROM t WHERE name = 'hello  world'";
        assertThat(AntlrSqlParser.normalizeCacheKey(sql1))
                .isNotEqualTo(AntlrSqlParser.normalizeCacheKey(sql2));
    }

    // ── Adaptive cache sizing tests ──

    @Test
    void adaptiveCacheSizeGrowsOnHighHitRate() {
        // Use a small initial cache to make adaptive behavior observable
        AntlrSqlParser smallParser = new AntlrSqlParser(1_048_576, 256, 256);
        assertThat(smallParser.currentMaxCacheSize()).isEqualTo(256);

        // Generate high hit rate: parse the same SQL repeatedly so every call after
        // the first is a cache hit, easily exceeding the 80% grow threshold.
        smallParser.parse("SELECT * FROM t WHERE id = 1");
        for (int i = 0; i < AntlrSqlParser.ADAPTIVE_EVAL_INTERVAL + 1; i++) {
            smallParser.parse("SELECT * FROM t WHERE id = 1");
        }
        // After evaluation interval with >80% hit rate, cache should grow
        assertThat(smallParser.currentMaxCacheSize()).isGreaterThan(256);
    }

    // ── Cache performance benchmark ──

    @Test
    void cachedParseIsFasterThanColdParse() {
        String sql = "SELECT a, b, c FROM my_table WHERE x > 1 AND y < 100 ORDER BY a";

        // Warm up: first parse (cold)
        parser.parse(sql);

        // Measure cold parse time (invalidate cache first)
        parser.invalidateCache();
        long coldStart = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            parser.invalidateCache();
            parser.parse(sql);
        }
        long coldElapsed = System.nanoTime() - coldStart;

        // Measure cached parse time
        parser.parse(sql); // ensure cached
        long hotStart = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            parser.parse(sql);
        }
        long hotElapsed = System.nanoTime() - hotStart;

        double speedup = (double) coldElapsed / hotElapsed;
        assertThat(speedup)
                .as("Cache speedup ratio (cold/hot): %.1fx", speedup)
                .isGreaterThan(3.0);
    }

    /**
     * Benchmark: cache hit rate must exceed 80% on a repeated workload where
     * the same SQL statements are executed many times. Each SQL string maps to
     * its own cache entry (literals are not stripped), so hits require the
     * exact same SQL text (whitespace-normalized).
     */
    @Test
    void cacheHitRateExceeds80PercentOnRepeatedWorkload() {
        AntlrSqlParser benchParser = new AntlrSqlParser();

        // Simulate a realistic workload: 10 distinct queries executed 20 times each.
        // First execution of each is a miss (10 misses), remaining 190 are hits =>
        // 95% hit rate.
        String[] queries = {
                "SELECT * FROM users WHERE id = 1",
                "SELECT name, email FROM users WHERE age > 18",
                "SELECT COUNT(*) FROM orders WHERE customer_id = 42",
                "SELECT * FROM products WHERE price < 100",
                "INSERT INTO logs (user_id, action) VALUES (1, 'login')",
                "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.id = 7",
                "DELETE FROM sessions WHERE user_id = 9",
                "UPDATE users SET last_login = '2024-01-01' WHERE id = 3",
                "SELECT * FROM events WHERE timestamp > 1700000000 ORDER BY timestamp",
                "SELECT DISTINCT category FROM products WHERE vendor_id = 5"
        };

        for (int round = 0; round < 20; round++) {
            for (String sql : queries) {
                benchParser.parse(sql);
            }
        }

        double hitRate = benchParser.cacheHitRate();
        assertThat(hitRate)
                .as("Cache hit rate on repeated workload: %.1f%%", hitRate * 100)
                .isGreaterThan(0.80);
    }
}
