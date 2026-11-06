package org.pangrova.parser.core.parse;

import org.pangrova.parser.api.diagnostic.ParseException;
import org.pangrova.parser.api.parse.SqlDialect;
import org.pangrova.parser.spi.parse.StatementParser;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.SourceLocation;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the statement parser extension mechanism (G-PARSE-05).
 *
 * <p>Verifies that custom statement parsers can be registered and used
 * to handle statement types not covered by the core ANTLR grammar,
 * without modifying the grammar itself.
 */
class StatementParserExtensionTest {

    // ── Custom statement for testing ──

    /** A simple custom statement AST node for testing. */
    record PingStatement(String target) implements Statement {
        @Override
        public Optional<SourceLocation> sourceLocation() {
            return Optional.empty();
        }
    }

    /** A custom statement parser that handles "PING <target>" statements. */
    static class PingStatementParser implements StatementParser {
        @Override
        public String name() {
            return "ping";
        }

        @Override
        public int priority() {
            return 100;
        }

        @Override
        public boolean canParse(String sql) {
            return sql.strip().toUpperCase().startsWith("PING ");
        }

        @Override
        public Statement parse(String sql, org.pangrova.parser.api.parse.SqlParser parentParser) {
            String trimmed = sql.strip();
            String target = trimmed.substring("PING ".length()).strip();
            if (target.isEmpty()) {
                throw new ParseException(
                        org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                        "PING requires a target", 1, 6);
            }
            return new PingStatement(target);
        }
    }

    /** Another custom parser for "HEALTH CHECK" statements. */
    static class HealthCheckParser implements StatementParser {
        @Override
        public String name() {
            return "health-check";
        }

        @Override
        public boolean canParse(String sql) {
            return sql.strip().toUpperCase().startsWith("HEALTH CHECK");
        }

        @Override
        public Statement parse(String sql, org.pangrova.parser.api.parse.SqlParser parentParser) {
            return new PingStatement("health");
        }
    }

    // ── Registry tests ──

    @Test
    void emptyRegistryFindsNoParser() {
        StatementParserRegistry registry = new StatementParserRegistry();
        assertThat(registry.findParser("PING server1")).isEmpty();
        assertThat(registry.size()).isZero();
    }

    @Test
    void registerAndFindParser() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        assertThat(registry.size()).isEqualTo(1);
        assertThat(registry.findParser("PING server1")).isPresent();
        assertThat(registry.findParser("SELECT 1")).isEmpty();
    }

    @Test
    void unregisterParser() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        assertThat(registry.size()).isEqualTo(1);

        boolean removed = registry.unregister("ping");
        assertThat(removed).isTrue();
        assertThat(registry.size()).isZero();
        assertThat(registry.findParser("PING server1")).isEmpty();
    }

    @Test
    void unregisterNonexistentReturnsFalse() {
        StatementParserRegistry registry = new StatementParserRegistry();
        assertThat(registry.unregister("nonexistent")).isFalse();
    }

    @Test
    void registerNullParserThrows() {
        StatementParserRegistry registry = new StatementParserRegistry();
        assertThatThrownBy(() -> registry.register(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null");
    }

    @Test
    void duplicateNameReplacesExistingParser() {
        StatementParserRegistry registry = new StatementParserRegistry();
        PingStatementParser first = new PingStatementParser();
        PingStatementParser second = new PingStatementParser();
        registry.register(first);
        registry.register(second);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void parsersReturnedInPriorityOrder() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new HealthCheckParser()); // default priority 1000
        registry.register(new PingStatementParser()); // priority 100

        assertThat(registry.registeredParsers())
                .extracting(StatementParser::name)
                .containsExactly("ping", "health-check");
    }

    // ── AntlrSqlParser integration tests ──

    @Test
    void extensionParserHandlesCustomStatement() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        Statement stmt = parser.parse("PING server1");
        assertThat(stmt).isInstanceOf(PingStatement.class);
        assertThat(((PingStatement) stmt).target()).isEqualTo("server1");
    }

    @Test
    void coreGrammarStillWorksWithExtensionsRegistered() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        // Standard SQL should still be handled by the core ANTLR grammar
        Statement stmt = parser.parse("SELECT 1");
        assertThat(stmt).isNotNull();
        assertThat(stmt).isNotInstanceOf(PingStatement.class);
    }

    @Test
    void extensionParserResultIsCached() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        Statement first = parser.parse("PING server1");
        Statement second = parser.parse("PING server1");
        // Cached — same object reference
        assertThat(second).isSameAs(first);
        assertThat(parser.cacheHitCount()).isEqualTo(1);
    }

    @Test
    void extensionParserCanDelegateBackToCore() {
        // An extension parser that wraps a core SELECT query
        StatementParser wrappingParser = new StatementParser() {
            @Override
            public String name() {
                return "explain-custom";
            }

            @Override
            public boolean canParse(String sql) {
                return sql.strip().toUpperCase().startsWith("CUSTOM EXPLAIN ");
            }

            @Override
            public Statement parse(String sql, org.pangrova.parser.api.parse.SqlParser parentParser) {
                // Delegate the inner query to the core parser
                String innerSql = sql.strip().substring("CUSTOM EXPLAIN ".length());
                Statement innerStmt = parentParser.parse(innerSql);
                // In a real implementation, wrap in an ExplainStatement-like node
                return innerStmt;
            }
        };

        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(wrappingParser);
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        // The extension parser delegates "SELECT 1" to the core parser
        Statement stmt = parser.parse("CUSTOM EXPLAIN SELECT 1");
        assertThat(stmt).isNotNull();
    }

    @Test
    void extensionParserErrorIsPropagated() {
        // Parser that claims any SQL starting with "PING" but rejects missing target
        StatementParser strictPingParser = new StatementParser() {
            @Override
            public String name() { return "strict-ping"; }

            @Override
            public int priority() { return 100; }

            @Override
            public boolean canParse(String sql) {
                return sql.strip().toUpperCase().startsWith("PING");
            }

            @Override
            public Statement parse(String sql, org.pangrova.parser.api.parse.SqlParser parentParser) {
                String trimmed = sql.strip();
                if (trimmed.length() <= 4 || trimmed.charAt(4) != ' ') {
                    throw new ParseException(
                            org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                            "PING requires a target", 1, 5);
                }
                String target = trimmed.substring(5).strip();
                if (target.isEmpty()) {
                    throw new ParseException(
                            org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                            "PING requires a target", 1, 5);
                }
                return new PingStatement(target);
            }
        };

        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(strictPingParser);
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        // PING with empty target should throw ParseException from the extension
        assertThatThrownBy(() -> parser.parse("PING"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("PING requires a target");

        // PING with only whitespace after it should also fail
        assertThatThrownBy(() -> parser.parse("PING   "))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("PING requires a target");
    }

    @Test
    void defaultConstructorHasEmptyRegistry() {
        AntlrSqlParser parser = new AntlrSqlParser();
        assertThat(parser.extensionRegistry()).isNotNull();
        assertThat(parser.extensionRegistry().size()).isZero();
    }

    @Test
    void multipleExtensionParsersCoexist() {
        StatementParserRegistry registry = new StatementParserRegistry();
        registry.register(new PingStatementParser());
        registry.register(new HealthCheckParser());
        AntlrSqlParser parser = new AntlrSqlParser(
                AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH,
                AntlrSqlParser.DEFAULT_MAX_TREE_DEPTH,
                AntlrSqlParser.DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA,
                registry);

        Statement ping = parser.parse("PING server1");
        assertThat(ping).isInstanceOf(PingStatement.class);
        assertThat(((PingStatement) ping).target()).isEqualTo("server1");

        Statement health = parser.parse("HEALTH CHECK");
        assertThat(health).isInstanceOf(PingStatement.class);
        assertThat(((PingStatement) health).target()).isEqualTo("health");

        // Standard SQL still works
        Statement select = parser.parse("SELECT 1");
        assertThat(select).isNotInstanceOf(PingStatement.class);
    }

    @Test
    void registryAccessorAllowsLateRegistration() {
        AntlrSqlParser parser = new AntlrSqlParser();

        // Core SQL works before registration
        Statement select = parser.parse("SELECT 1");
        assertThat(select).isNotNull();

        // Register extension after parser creation
        parser.extensionRegistry().register(new PingStatementParser());

        // Now custom statement works too
        // (invalidate cache to avoid stale entries)
        parser.invalidateCache();
        Statement ping = parser.parse("PING server1");
        assertThat(ping).isInstanceOf(PingStatement.class);
    }
}
