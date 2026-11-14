package org.pangrova.parser.api.enums;

/**
 * SQL dialect configuration controlling lexer behavior:
 * identifier quoting style and case sensitivity for unquoted identifiers.
 *
 * <p>Industry reference points:
 * <ul>
 *   <li><b>ANSI SQL</b>: double-quote delimited identifiers, unquoted identifiers
 *       fold to UPPER CASE (SQL:2016 Part 2, Section 5.2).</li>
 *   <li><b>Trino/Presto</b>: double-quote delimited identifiers, unquoted identifiers
 *       fold to lower case (Trino convention).</li>
 *   <li><b>Spark SQL</b>: backtick delimited identifiers, case-preserving
 *       (case-insensitive resolution happens downstream).</li>
 *   <li><b>Pangrova</b> (default): accepts both double-quotes and backticks,
 *       case-preserving — the most permissive mode.</li>
 * </ul>
 *
 * <p>Quoted identifiers always preserve their exact case regardless of dialect.
 * Case folding applies only to unquoted identifiers.
 */
public enum SqlDialect {

    /**
     * ANSI SQL standard: double-quote quoting only, unquoted identifiers fold
     * to UPPER CASE per SQL:2016 Section 5.2.
     */
    ANSI(QuotingStyle.DOUBLE_QUOTES, CaseSensitivity.UPPER),

    /**
     * Trino/Presto: double-quote quoting only, unquoted identifiers fold
     * to lower case (Trino convention).
     */
    TRINO(QuotingStyle.DOUBLE_QUOTES, CaseSensitivity.LOWER),

    /**
     * Spark SQL: backtick quoting, unquoted identifiers are case-preserving
     * (case-insensitive resolution is deferred to the downstream resolver).
     */
    SPARK(QuotingStyle.BACKTICKS, CaseSensitivity.PRESERVE),

    /**
     * Default Pangrova mode: both quoting styles accepted, case-preserving.
     * This is the most permissive mode and the default for backward compatibility.
     */
    PANGROVA(QuotingStyle.BOTH, CaseSensitivity.PRESERVE);

    private final QuotingStyle quotingStyle;
    private final CaseSensitivity caseSensitivity;

    SqlDialect(QuotingStyle quotingStyle, CaseSensitivity caseSensitivity) {
        this.quotingStyle = quotingStyle;
        this.caseSensitivity = caseSensitivity;
    }

    /** Returns the identifier quoting style for this dialect. */
    public QuotingStyle quotingStyle() {
        return quotingStyle;
    }

    /** Returns the case sensitivity policy for unquoted identifiers. */
    public CaseSensitivity caseSensitivity() {
        return caseSensitivity;
    }

    /**
     * Whether a double-quoted identifier is accepted in this dialect.
     */
    public boolean allowsDoubleQuotes() {
        return quotingStyle == QuotingStyle.DOUBLE_QUOTES
                || quotingStyle == QuotingStyle.BOTH;
    }

    /**
     * Whether a backtick-quoted identifier is accepted in this dialect.
     */
    public boolean allowsBackticks() {
        return quotingStyle == QuotingStyle.BACKTICKS
                || quotingStyle == QuotingStyle.BOTH;
    }

    /**
     * Apply case folding to an unquoted identifier according to this dialect.
     * Quoted identifiers are never folded — callers must not pass quoted text.
     */
    public String foldIdentifier(String identifier) {
        return switch (caseSensitivity) {
            case UPPER -> identifier.toUpperCase(java.util.Locale.ROOT);
            case LOWER -> identifier.toLowerCase(java.util.Locale.ROOT);
            case PRESERVE -> identifier;
        };
    }

    /**
     * Normalize a quoted identifier to this dialect's canonical quoting style.
     *
     * <p>Backtick-quoted identifiers ({@code `col`}) are converted to double-quoted
     * form ({@code "col"}) when the dialect uses double-quotes (ANSI, TRINO).
     * Double-quoted identifiers are converted to backtick form when the dialect
     * uses backticks only (SPARK). In the PANGROVA dialect (both accepted), the
     * identifier is returned unchanged.
     *
     * <p>Unquoted identifiers are returned unchanged — this method only handles
     * quote-style conversion.
     *
     * <p>Industry note: this is the equivalent of Trino's
     * {@code SqlFormatter.formatName()} and Spark's {@code UnresolvedAttribute}
     * quote normalization. Useful for SQL-to-SQL translation between dialects.
     *
     * @param quotedIdentifier the quoted identifier text (including delimiters)
     * @return the identifier in this dialect's canonical quoting style
     */
    public String normalizeQuoting(String quotedIdentifier) {
        if (quotedIdentifier == null || quotedIdentifier.length() < 2) {
            return quotedIdentifier;
        }
        char first = quotedIdentifier.charAt(0);
        char last = quotedIdentifier.charAt(quotedIdentifier.length() - 1);

        if (first == '`' && last == '`') {
            // Backtick-quoted → normalize to target style
            if (quotingStyle == QuotingStyle.DOUBLE_QUOTES) {
                String inner = quotedIdentifier.substring(1, quotedIdentifier.length() - 1)
                        .replace("``", "`");
                return "\"" + inner.replace("\"", "\"\"") + "\"";
            }
            return quotedIdentifier; // BACKTICKS or BOTH: keep as-is
        }
        if (first == '"' && last == '"') {
            // Double-quoted → normalize to target style
            if (quotingStyle == QuotingStyle.BACKTICKS) {
                String inner = quotedIdentifier.substring(1, quotedIdentifier.length() - 1)
                        .replace("\"\"", "\"");
                return "`" + inner.replace("`", "``") + "`";
            }
            return quotedIdentifier; // DOUBLE_QUOTES or BOTH: keep as-is
        }
        // Not a quoted identifier (or unrecognized quoting) — return unchanged
        return quotedIdentifier;
    }

    /**
     * Identifier quoting style.
     */
    public enum QuotingStyle {
        /** Only double-quote delimited identifiers ({@code "col"}). */
        DOUBLE_QUOTES,
        /** Only backtick delimited identifiers ({@code `col`}). */
        BACKTICKS,
        /** Both double-quotes and backticks accepted. */
        BOTH
    }

    /**
     * Case sensitivity policy for unquoted identifiers.
     * Quoted identifiers always preserve their exact case.
     */
    public enum CaseSensitivity {
        /** Preserve original case (Pangrova/Spark default). */
        PRESERVE,
        /** Fold unquoted identifiers to UPPER case (ANSI standard). */
        UPPER,
        /** Fold unquoted identifiers to lower case (Trino/Presto convention). */
        LOWER
    }
}
