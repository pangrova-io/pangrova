package org.pangrova.parser.parse;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.api.parse.ParseResult;
import org.pangrova.parser.api.enums.SqlDialect;
import org.pangrova.parser.build.antlr.PangrovaSqlLexer;
import org.pangrova.parser.build.antlr.PangrovaSqlParser;
import org.pangrova.parser.build.AstBuilder;
import org.pangrova.parser.diagnostic.CollectingErrorListener;
import org.pangrova.parser.diagnostic.ThrowingErrorListener;
import org.pangrova.parser.lexical.ReservedWordPolicy;
import org.pangrova.parser.api.parse.StatementParser;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.api.ast.Statement;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.pangrova.common.util.SqlText;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry point for parsing SQL text into an AST {@link Statement}.
 * Delegates to the ANTLR4-generated lexer/parser and {@link AstBuilder}.
 *
 * <p>Performance optimizations:
 * <ul>
 *   <li>Adaptive-size LRU parse cache keyed by whitespace-normalized SQL (Caffeine).
 *       Cache size grows when hit rate exceeds 80% and shrinks when below 50%.</li>
 *   <li>SLL prediction mode first, falling back to LL on ambiguity.
 *       Statistics track SLL success rate to validate the fast-path effectiveness.</li>
 * </ul>
 *
 * <p><b>Note on cache key normalization:</b> The cache key normalizes whitespace
 * only and intentionally preserves literal values. Earlier versions stripped
 * numeric and string literals to maximize hit rate, but because the cached AST
 * objects embed concrete literal values, two SQL strings differing only in
 * literals would resolve to the same cache entry and return an AST containing
 * the wrong literal values. Parameterized caching would require an AST that
 * carries placeholders rather than concrete literals.</p>
 *
 * <p>Enforces input limits to prevent resource exhaustion:
 * <ul>
 *   <li>Maximum SQL text length (default 1 MB)</li>
 *   <li>Maximum parse tree depth (default 256)</li>
 * </ul>
 */
@org.pangrova.common.concurrent.ThreadSafe
public class AntlrSqlParser implements org.pangrova.parser.api.parse.SqlParser {

    /** Default maximum SQL text length in bytes: 1 MB. */
    static final int DEFAULT_MAX_SQL_LENGTH = 1_048_576;

    /** Default maximum parse tree depth. */
    static final int DEFAULT_MAX_TREE_DEPTH = 256;

    /** Default initial number of cached parse results. */
    static final int DEFAULT_CACHE_INITIAL_SIZE = 1000;

    /** Minimum cache size during adaptive shrink. */
    static final int CACHE_MIN_SIZE = 256;

    /** Maximum cache size during adaptive growth. */
    static final int CACHE_MAX_SIZE = 8192;

    /** Hit rate threshold above which cache grows (80%). */
    static final double HIT_RATE_GROW_THRESHOLD = 0.80;

    /** Hit rate threshold below which cache shrinks (50%). */
    static final double HIT_RATE_SHRINK_THRESHOLD = 0.50;

    /** Number of requests between adaptive resize evaluations. */
    static final long ADAPTIVE_EVAL_INTERVAL = 500;

    private final int maxSqlLength;
    private final int maxTreeDepth;
    private final SqlDialect dialect;
    private final StatementParserRegistry extensionRegistry;
    private record ParseCacheKey(long extensionRevision, String sql) {}

    private volatile Cache<ParseCacheKey, Statement> parseCache;
    private volatile int currentMaxSize;

    // Cache hit/miss statistics for adaptive sizing
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong totalRequests = new AtomicLong();

    // SLL prediction statistics
    private final AtomicLong sllSuccessCount = new AtomicLong();
    private final AtomicLong sllFallbackCount = new AtomicLong();

    public AntlrSqlParser() {
        this(DEFAULT_MAX_SQL_LENGTH, DEFAULT_MAX_TREE_DEPTH, DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA, new StatementParserRegistry());
    }

    public AntlrSqlParser(SqlDialect dialect) {
        this(DEFAULT_MAX_SQL_LENGTH, DEFAULT_MAX_TREE_DEPTH, DEFAULT_CACHE_INITIAL_SIZE,
                dialect, new StatementParserRegistry());
    }

    public AntlrSqlParser(int maxSqlLength, int maxTreeDepth) {
        this(maxSqlLength, maxTreeDepth, DEFAULT_CACHE_INITIAL_SIZE,
                SqlDialect.PANGROVA, new StatementParserRegistry());
    }

    public AntlrSqlParser(int maxSqlLength, int maxTreeDepth, int cacheMaxSize) {
        this(maxSqlLength, maxTreeDepth, cacheMaxSize,
                SqlDialect.PANGROVA, new StatementParserRegistry());
    }

    /**
     * Creates a parser with explicit resource limits, dialect, and extension registry.
     *
     * @param maxSqlLength maximum accepted SQL text length
     * @param maxTreeDepth maximum accepted parse-tree depth
     * @param cacheMaxSize maximum number of cached parse results
     * @param dialect SQL dialect
     * @param extensionRegistry statement extension registry
     */
    public AntlrSqlParser(int maxSqlLength, int maxTreeDepth, int cacheMaxSize,
                     SqlDialect dialect, StatementParserRegistry extensionRegistry) {
        if (maxSqlLength <= 0) {
            throw new IllegalArgumentException("maxSqlLength must be positive");
        }
        if (maxTreeDepth <= 0) {
            throw new IllegalArgumentException("maxTreeDepth must be positive");
        }
        if (cacheMaxSize < 0) {
            throw new IllegalArgumentException("cacheMaxSize must be non-negative");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        this.maxSqlLength = maxSqlLength;
        this.maxTreeDepth = maxTreeDepth;
        this.dialect = dialect;
        this.extensionRegistry = extensionRegistry != null
                ? extensionRegistry : new StatementParserRegistry();
        this.currentMaxSize = cacheMaxSize;
        this.parseCache = Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .build();
    }

    /**
     * Parse a SQL string into an AST Statement.
     * Results are cached by normalized SQL key (literal contents preserved) for repeated queries.
     * The cache adaptively resizes based on observed hit rate.
     *
     * @throws ParseException with line/column on syntax error or input limit violation
     */
    public Statement parse(String sql) {
        validateInput(sql);
        var extensions = extensionRegistry.snapshot();
        // An extension owns its syntax and can distinguish whitespace in arbitrary positions.
        String keyText = extensions.parsers().isEmpty() ? SqlText.parseCacheKey(sql) : sql;
        var cacheKey = new ParseCacheKey(extensions.revision(), keyText);
        Statement cached = parseCache.getIfPresent(cacheKey);
        if (cached != null) {
            cacheHits.incrementAndGet();
            totalRequests.incrementAndGet();
            maybeAdaptCacheSize();
            return cached;
        }
        cacheMisses.incrementAndGet();
        totalRequests.incrementAndGet();

        // Check extension parsers first
        java.util.Optional<StatementParser> extParser = extensions.findParser(sql);
        Statement result;
        if (extParser.isPresent()) {
            result = extParser.get().parse(sql, this);
        } else {
            result = parseInternal(sql);
        }

        parseCache.put(cacheKey, result);
        maybeAdaptCacheSize();
        return result;
    }

    /**
     * Returns the current number of entries in the parse cache.
     * Intended for testing and monitoring.
     */
    public long cacheSize() {
        return parseCache.estimatedSize();
    }

    /**
     * Returns the current maximum cache size (may change due to adaptive resizing).
     */
    public int currentMaxCacheSize() {
        return currentMaxSize;
    }

    /**
     * Returns the cache hit rate as a value between 0.0 and 1.0.
     * Returns 0.0 if no requests have been made.
     */
    public double cacheHitRate() {
        long total = totalRequests.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) cacheHits.get() / total;
    }

    /**
     * Returns the SLL prediction success rate as a value between 0.0 and 1.0.
     * A high rate indicates the grammar is mostly unambiguous for the workload.
     * Returns 1.0 if no parse attempts have been made (no fallbacks).
     */
    public double sllSuccessRate() {
        long successes = sllSuccessCount.get();
        long fallbacks = sllFallbackCount.get();
        long total = successes + fallbacks;
        if (total == 0) {
            return 1.0;
        }
        return (double) successes / total;
    }

    /**
     * Returns total cache hits since creation or last reset.
     */
    public long cacheHitCount() {
        return cacheHits.get();
    }

    /**
     * Returns total cache misses since creation or last reset.
     */
    public long cacheMissCount() {
        return cacheMisses.get();
    }

    /**
     * Invalidates all entries in the parse cache and resets statistics.
     */
    public void invalidateCache() {
        parseCache.invalidateAll();
        cacheHits.set(0);
        cacheMisses.set(0);
        totalRequests.set(0);
    }

    /**
     * Returns the SQL dialect configured for this parser.
     */
    public SqlDialect dialect() {
        return dialect;
    }

    /**
     * Returns the extension parser registry for this parser.
     * Allows late registration of custom statement parsers.
     */
    public StatementParserRegistry extensionRegistry() {
        return extensionRegistry;
    }

    /**
     * Convenience method: returns true if the given word is a reserved keyword
     * in the Pangrova SQL grammar. Delegates to {@link ReservedWordPolicy#isReserved(String)}.
     *
     * @param word the word to check
     * @return true if it is a reserved keyword
     */
    public static boolean isReserved(String word) {
        return ReservedWordPolicy.INSTANCE.isReserved(word);
    }

    /**
     * Returns the singleton {@link ReservedWordPolicy} instance used by this parser.
     */
    public static ReservedWordPolicy reservedWordPolicy() {
        return ReservedWordPolicy.INSTANCE;
    }

    /**
     * Parse SQL in error-recovery mode, producing a partial AST with error markers
     * instead of throwing on the first syntax error. This enables IDE-style incremental
     * parsing and better error messages for malformed SQL.
     *
     * <p>Uses ANTLR's {@code DefaultErrorStrategy} which attempts to recover from
     * syntax errors by inserting/deleting tokens, producing a parse tree that covers
     * as much of the input as possible.
     *
     * <p>Results from this method are NOT cached, since partial/error results should
     * not pollute the cache used by the strict {@link #parse(String)} method.
     *
     * @param sql the SQL text to parse
     * @return a {@link ParseResult} containing the statement (possibly null) and any errors
     * @throws ParseException if the input violates size limits (not a syntax error)
     */
    public ParseResult parseWithRecovery(String sql) {
        return parseWithDetails(sql).result();
    }

    /** Preserves the ANTLR recovery tree for explicit implementation-specific tooling. */
    public org.pangrova.parser.diagnostic.RecoveryParseDetails parseWithDetails(String sql) {
        validateInput(sql);

        PangrovaSqlLexer lexer = new PangrovaSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        CollectingErrorListener errorListener = new CollectingErrorListener();
        lexer.addErrorListener(errorListener);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        if (tokens.getNumberOfOnChannelTokens() > maxTreeDepth * 256) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "Token count " + tokens.getNumberOfOnChannelTokens()
                            + " suggests excessive nesting (depth limit " + maxTreeDepth + ")",
                    0, 0);
        }

        try {
            PangrovaSqlParser parser = new PangrovaSqlParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);

            PangrovaSqlParser.SingleStatementContext tree = parser.singleStatement();
            enforceDepthLimit(tree);

            Statement statement = null;
            if (!errorListener.hasErrors()) {
                try {
                    statement = new AstBuilder(tokens, dialect).visitSingleStatement(tree);
                    return org.pangrova.parser.diagnostic.RecoveryParseDetails.success(statement, tree);
                } catch (ParseException syntax) {
                    if (syntax.getErrorCode() != PangrovaErrorCode.PARSER_SYNTAX_ERROR) throw syntax;
                    // Literal/identifier validation can reject an otherwise valid ANTLR tree.
                    // Preserve its position and tree just like a grammar syntax error; do not mask resource limits.
                    var diagnostic = new org.pangrova.parser.api.diagnostic.ParseError(
                            syntax.getLine(), syntax.getColumn(), syntax.getMessage());
                    return org.pangrova.parser.diagnostic.RecoveryParseDetails.withErrors(
                            null, tree, java.util.List.of(diagnostic));
                }
            }

            // Attempt to build a partial AST despite errors
            try {
                statement = new AstBuilder(tokens, dialect).visitSingleStatement(tree);
            } catch (@SuppressWarnings("unused") Exception astBuildFailure) {
                // AST building failed on the error-recovery tree — statement remains null.
                // Error details are already captured in errorListener.getErrors().
            }

            return org.pangrova.parser.diagnostic.RecoveryParseDetails.withErrors(
                    statement, tree, errorListener.getErrors());
        } catch (StackOverflowError e) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "Parse tree depth exceeds maximum " + maxTreeDepth
                            + " (stack overflow during parsing)",
                    0, 0);
        }
    }

    private Statement parseInternal(String sql) {
        PangrovaSqlLexer lexer = new PangrovaSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        if (tokens.getNumberOfOnChannelTokens() > maxTreeDepth * 256) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "Token count " + tokens.getNumberOfOnChannelTokens()
                            + " suggests excessive nesting (depth limit " + maxTreeDepth + ")",
                    0, 0);
        }

        try {
            PangrovaSqlParser.SingleStatementContext tree = parseWithSllFallback(tokens);
            enforceDepthLimit(tree);
            return new AstBuilder(tokens, dialect).visitSingleStatement(tree);
        } catch (StackOverflowError e) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "Parse tree depth exceeds maximum " + maxTreeDepth
                            + " (stack overflow during parsing)",
                    0, 0);
        }
    }

    /**
     * Evaluate cache hit rate periodically and resize the cache adaptively.
     * Grows the cache (up to {@link #CACHE_MAX_SIZE}) when hit rate is high,
     * shrinks it (down to {@link #CACHE_MIN_SIZE}) when hit rate is low.
     */
    private void maybeAdaptCacheSize() {
        long total = totalRequests.get();
        if (total == 0 || total % ADAPTIVE_EVAL_INTERVAL != 0) {
            return;
        }
        double hitRate = (double) cacheHits.get() / total;
        int newSize = currentMaxSize;
        if (hitRate > HIT_RATE_GROW_THRESHOLD && currentMaxSize < CACHE_MAX_SIZE) {
            newSize = Math.min(currentMaxSize * 2, CACHE_MAX_SIZE);
        } else if (hitRate < HIT_RATE_SHRINK_THRESHOLD && currentMaxSize > CACHE_MIN_SIZE) {
            newSize = Math.max(currentMaxSize / 2, CACHE_MIN_SIZE);
        }
        if (newSize != currentMaxSize) {
            currentMaxSize = newSize;
            Cache<ParseCacheKey, Statement> oldCache = parseCache;
            parseCache = Caffeine.newBuilder()
                    .maximumSize(newSize)
                    .build();
            // Migrate existing entries to the new cache
            oldCache.asMap().forEach((key, value) -> parseCache.put(key, value));
        }
    }

    private void validateInput(String sql) {
        if (sql == null) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "SQL text must not be null", 0, 0);
        }
        if (sql.length() > maxSqlLength) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "SQL text length " + sql.length()
                            + " exceeds maximum " + maxSqlLength,
                    0, 0);
        }
    }

    /**
     * Attempt parsing with SLL prediction mode (fast path). If the grammar
     * is ambiguous for the given input, catch the failure and retry with
     * full LL prediction mode for correctness. Tracks SLL success/fallback
     * statistics for monitoring prediction mode effectiveness.
     */
    private PangrovaSqlParser.SingleStatementContext parseWithSllFallback(CommonTokenStream tokens) {
        PangrovaSqlParser sllParser = new PangrovaSqlParser(tokens);
        sllParser.removeErrorListeners();
        sllParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        sllParser.setErrorHandler(new BailErrorStrategy());

        try {
            PangrovaSqlParser.SingleStatementContext result = sllParser.singleStatement();
            sllSuccessCount.incrementAndGet();
            return result;
        } catch (ParseCancellationException e) {
            // SLL failed due to ambiguity or syntax error — retry with full LL mode
            sllFallbackCount.incrementAndGet();
            tokens.seek(0);
            return parseWithLlMode(tokens);
        }
    }

    /**
     * Parse with full LL prediction mode and standard error reporting.
     */
    private PangrovaSqlParser.SingleStatementContext parseWithLlMode(CommonTokenStream tokens) {
        PangrovaSqlParser llParser = new PangrovaSqlParser(tokens);
        llParser.removeErrorListeners();
        llParser.addErrorListener(ThrowingErrorListener.INSTANCE);
        llParser.getInterpreter().setPredictionMode(PredictionMode.LL);

        return llParser.singleStatement();
    }

    private void enforceDepthLimit(PangrovaSqlParser.SingleStatementContext tree) {
        int depth = measureDepth(tree);
        if (depth > maxTreeDepth) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_INPUT_TOO_LARGE,
                    "Parse tree depth " + depth
                            + " exceeds maximum " + maxTreeDepth,
                    0, 0);
        }
    }

    /**
     * Iteratively measure the maximum depth of a parse tree to avoid
     * stack overflow on deeply nested input.
     */
    static int measureDepth(ParseTree root) {
        int maxDepth = 0;
        java.util.ArrayDeque<ParseTree> nodeStack = new java.util.ArrayDeque<>();
        java.util.ArrayDeque<Integer> depthStack = new java.util.ArrayDeque<>();
        nodeStack.push(root);
        depthStack.push(1);

        while (!nodeStack.isEmpty()) {
            ParseTree node = nodeStack.pop();
            int d = depthStack.pop();
            if (d > maxDepth) {
                maxDepth = d;
            }
            for (int i = 0; i < node.getChildCount(); i++) {
                nodeStack.push(node.getChild(i));
                depthStack.push(d + 1);
            }
        }
        return maxDepth;
    }
}
