package org.pangrova.parser;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Classifies SQL keywords as {@link Category#RESERVED} or {@link Category#NON_RESERVED}
 * according to the Pangrova SQL grammar ({@code PangrovaSql.g4}).
 *
 * <h3>Terminology</h3>
 * <ul>
 *   <li><b>Reserved keyword</b>: cannot be used as an unquoted identifier; the parser
 *       treats it as syntax. Example: {@code SELECT}, {@code FROM}, {@code WHERE}.
 *       To use a reserved keyword as an identifier, it must be quoted
 *       (double-quotes or backticks, depending on dialect).</li>
 *   <li><b>Non-reserved keyword</b>: recognized as a keyword in certain syntactic
 *       positions but can also be used as an unquoted identifier. Example:
 *       {@code CATALOG}, {@code REFRESH}, {@code STATUS}.
 *       The grammar's {@code nonReservedKeyword} rule lists these.</li>
 * </ul>
 *
 * <h3>ANSI SQL comparison</h3>
 * <p>The SQL:2016 standard (Part 2, Section 5.2) distinguishes reserved words from
 * non-reserved words. Pangrova's reserved set is intentionally minimal — only keywords
 * that are structurally ambiguous with identifiers are reserved. Most DDL/DML action
 * verbs (CREATE, DROP, INSERT, DELETE, etc.) appear in the non-reserved list because
 * the grammar can distinguish them from identifiers by context.</p>
 *
 * <h3>Thread safety</h3>
 * <p>This class is immutable and thread-safe. The singleton {@link #INSTANCE} can be
 * shared across threads without synchronization.</p>
 *
 * @see SqlDialect
 * @see SqlParser#isReserved(String)
 */
public final class ReservedWordPolicy {

    /**
     * Keyword classification.
     */
    public enum Category {
        /**
         * The keyword is reserved: it cannot be used as an unquoted identifier.
         * Must be quoted (double-quotes or backticks, per dialect) to use as an
         * identifier.
         */
        RESERVED,

        /**
         * The keyword is non-reserved: it is recognized as a keyword in certain
         * syntactic positions but can also be used as an unquoted identifier
         * without quoting.
         */
        NON_RESERVED
    }

    /** Singleton instance — the keyword classification is derived from the grammar. */
    public static final ReservedWordPolicy INSTANCE = new ReservedWordPolicy();

    /*
     * Reserved keywords — those that appear as lexer tokens in the grammar but are
     * NOT listed in the nonReservedKeyword parser rule.  These cannot be used as
     * unquoted identifiers.
     *
     * Derived from PangrovaSql.g4 by comparing the full keyword token list against
     * the nonReservedKeyword alternatives.
     */
    private static final Set<String> RESERVED_WORDS;

    /*
     * Non-reserved keywords — those listed in the nonReservedKeyword parser rule.
     * Can be used as unquoted identifiers.
     */
    private static final Set<String> NON_RESERVED_WORDS;

    /** Combined map: keyword (upper-case) -> category. */
    private static final Map<String, Category> KEYWORD_MAP;

    static {
        // ── Reserved keywords ──
        // These tokens appear in the grammar as structural syntax and are NOT
        // listed in the nonReservedKeyword rule.
        Set<String> reserved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        reserved.addAll(java.util.List.of(
                "SELECT", "FROM", "WHERE", "GROUP", "BY",
                "HAVING", "ORDER",
                "AS", "ON",
                "AND", "OR", "NOT",
                "TRUE", "FALSE", "NULL",
                "ASC", "DESC",
                // Join keywords (except OUTER which is non-reserved)
                "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS",
                // Set operations (UNION/ALL reserved; INTERSECT/EXCEPT are non-reserved)
                "UNION", "ALL",
                // Subquery
                "IN", "DISTINCT",
                // DDL verbs (CREATE/DROP/ALTER are NOT in nonReservedKeyword;
                // SHOW IS non-reserved per the grammar)
                "CREATE", "DROP", "ALTER",
                // OF
                "OF",
                // WHEN (MERGE syntax)
                "WHEN",
                // AI_EMBED (compound token)
                "AI_EMBED",
                // CAST and function-like keywords
                "CAST"
        ));
        RESERVED_WORDS = Collections.unmodifiableSet(reserved);

        // ── Non-reserved keywords ──
        // Exactly mirrors the nonReservedKeyword rule in PangrovaSql.g4.
        Set<String> nonReserved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        nonReserved.addAll(java.util.List.of(
                // Extension/DDL
                "REFRESH", "MATERIALIZED", "VIEW", "INCREMENTAL",
                "COMPACT", "OPTIMIZE", "VACUUM",
                "CATALOG", "CATALOGS", "DESCRIBE", "PROPERTIES", "CALL",
                // Time travel
                "SYSTEM_TIME", "SYSTEM_VERSION", "VERSION", "SET", "WITH",
                "SHOW", // Note: SHOW appears in both but grammar context disambiguates
                "LIMIT",
                // Table DDL
                "TABLE", "IF", "EXISTS", "INTO", "VALUES", "INSERT",
                "PARTITIONED", "COMMENT", "TBLPROPERTIES",
                // Types
                "BIGINT", "INT", "VARCHAR", "DOUBLE", "BOOLEAN", "DATE",
                "OFFSET", "TIMESTAMP", "STRING", "INTEGER",
                "OUTER", "OVER", "PARTITION",
                "DECIMAL", "NUMERIC", "FLOAT", "TINYINT", "SMALLINT",
                "VECTOR", "ARRAY", "MAP", "STRUCT",
                // Constraints
                "PRIMARY", "KEY", "ENFORCED",
                // DML
                "DELETE", "UPDATE", "MERGE", "USING", "MATCHED", "THEN", "IS",
                // Additional
                "OVERWRITE", "ADD", "COLUMN", "RENAME", "TO",
                "SCHEMAS", "SCHEMA", "TABLES",
                "DATA", "TYPE", "LIKE", "BETWEEN", "CASE", "ELSE", "END", "DEFAULT",
                // Window frame
                "ROWS", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW",
                // Set ops (non-reserved in Pangrova)
                "INTERSECT", "EXCEPT",
                // Dynamic table
                "DYNAMIC", "TARGET_LAG", "REFRESH_MODE", "WAREHOUSE", "REDEFINE",
                "SUSPEND", "RESUME", "CASCADE", "CDC", "AUTO", "SNAPSHOT",
                // Time units
                "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
                // Functions
                "INTERVAL", "EXTRACT", "SUBSTRING",
                // Explain
                "EXPLAIN", "SPEC",
                // Grouping
                "GROUPING", "SETS", "CUBE", "ROLLUP",
                // Lateral
                "LATERAL", "EXPLODE", "POSEXPLODE",
                // Index
                "INDEX", "INDEXES", "REBUILD", "FORCE",
                // Clustering
                "CLUSTERING", "RECLUSTER", "INTERLEAVE", "LINEAR", "MODE", "STATUS",
                "FOR",
                // Stream
                "STREAM", "STREAMS", "KAFKA", "FORMAT",
                // Variant/geo types
                "VARIANT", "GEOMETRY", "GEOGRAPHY", "TIMESTAMP_NS",
                // Metadata
                "VIEWS", "SNAPSHOTS", "FILES", "MANIFESTS", "PARTITIONS",
                "BRANCH", "TAG", "SORTED", "ZORDER",
                "BRANCHES", "TAGS", "MAIN", "USE", "UNSET",
                "CHANGES", "MATCH",
                // Embedding
                "EMBEDDING", "MODEL", "MODELS",
                "GENERATED", "ALWAYS", "SYNC", "ASYNC",
                // Rerank
                "RERANK", "RRF", "LINEAR_COMBINATION", "CROSS_ENCODER",
                // Backfill
                "BACKFILL", "BATCH_SIZE", "CONCURRENCY", "CANCEL",
                // BLOB functions
                "BLOB", "FROM_FILE", "TO_FILE", "BLOB_SIZE", "BLOB_MIME_TYPE",
                // Feature engineering
                "TOKENIZE", "NORMALIZE", "ONE_HOT_ENCODE", "BUCKETIZE",
                // CTE
                "RECURSIVE",
                "SEARCH", "DEPTH", "BREADTH", "CYCLE",
                // Temporal
                "TIME", "ZONE",
                // QUALIFY
                "QUALIFY",
                // TABLESAMPLE
                "TABLESAMPLE", "BERNOULLI", "REPEATABLE", "SYSTEM",
                // Window
                "WINDOW", "RANGE",
                // PIVOT/UNPIVOT
                "PIVOT", "UNPIVOT", "INCLUDE", "EXCLUDE", "NULLS"
        ));
        // SHOW appears in both reserved (as statement leader) and non-reserved
        // (as usable identifier per grammar). The grammar resolves it by context.
        // We classify it as NON_RESERVED since the grammar allows it as an identifier.
        NON_RESERVED_WORDS = Collections.unmodifiableSet(nonReserved);

        // Build combined map
        Map<String, Category> map = new LinkedHashMap<>();
        for (String word : reserved) {
            map.put(word.toUpperCase(Locale.ROOT), Category.RESERVED);
        }
        for (String word : nonReserved) {
            map.put(word.toUpperCase(Locale.ROOT), Category.NON_RESERVED);
        }
        KEYWORD_MAP = Collections.unmodifiableMap(map);
    }

    private ReservedWordPolicy() {
    }

    /**
     * Returns whether the given word is a reserved keyword. Reserved keywords
     * cannot be used as unquoted identifiers — they must be quoted.
     *
     * <p>The check is case-insensitive.
     *
     * @param word the word to check
     * @return true if the word is a reserved keyword
     */
    public boolean isReserved(String word) {
        if (word == null || word.isEmpty()) {
            return false;
        }
        return RESERVED_WORDS.contains(word);
    }

    /**
     * Returns whether the given word is a non-reserved keyword. Non-reserved
     * keywords can be used as unquoted identifiers.
     *
     * <p>The check is case-insensitive.
     *
     * @param word the word to check
     * @return true if the word is a non-reserved keyword
     */
    public boolean isNonReserved(String word) {
        if (word == null || word.isEmpty()) {
            return false;
        }
        return NON_RESERVED_WORDS.contains(word);
    }

    /**
     * Returns whether the given word is any keyword (reserved or non-reserved).
     *
     * <p>The check is case-insensitive.
     *
     * @param word the word to check
     * @return true if the word is a keyword
     */
    public boolean isKeyword(String word) {
        return isReserved(word) || isNonReserved(word);
    }

    /**
     * Returns the category of a keyword, or {@code null} if the word is not
     * a keyword.
     *
     * <p>The check is case-insensitive.
     *
     * @param word the word to check
     * @return the category, or null if not a keyword
     */
    public Category classify(String word) {
        if (word == null || word.isEmpty()) {
            return null;
        }
        return KEYWORD_MAP.get(word.toUpperCase(Locale.ROOT));
    }

    /**
     * Returns an unmodifiable set of all reserved keywords (upper-case).
     */
    public Set<String> reservedWords() {
        return RESERVED_WORDS.stream()
                .map(w -> w.toUpperCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns an unmodifiable set of all non-reserved keywords (upper-case).
     */
    public Set<String> nonReservedWords() {
        return NON_RESERVED_WORDS.stream()
                .map(w -> w.toUpperCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns an unmodifiable set of all keywords (both reserved and
     * non-reserved), upper-case.
     */
    public Set<String> allKeywords() {
        return Collections.unmodifiableSet(KEYWORD_MAP.keySet());
    }

    /**
     * Returns the full keyword map: keyword (upper-case) to category.
     */
    public Map<String, Category> keywordMap() {
        return KEYWORD_MAP;
    }

    /**
     * Quotes an identifier if it conflicts with a reserved keyword, using
     * the quoting style appropriate for the given dialect.
     *
     * <p>If the identifier is not a reserved keyword, it is returned unchanged.
     * If the dialect supports double-quotes (ANSI, TRINO, PANGROVA), the identifier
     * is double-quoted. If the dialect only supports backticks (SPARK), it is
     * backtick-quoted.
     *
     * @param identifier the identifier to conditionally quote
     * @param dialect    the SQL dialect to determine quoting style
     * @return the identifier, quoted if it is a reserved keyword
     */
    public String quoteIfReserved(String identifier, SqlDialect dialect) {
        if (!isReserved(identifier)) {
            return identifier;
        }
        if (dialect.allowsDoubleQuotes()) {
            return "\"" + identifier.replace("\"", "\"\"") + "\"";
        }
        return "`" + identifier.replace("`", "``") + "`";
    }
}
