package org.pangrova.client.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Inlines bound parameter values into SQL text with SQL-safe escaping.
 * Package-private helper extracted from {@link PangrovaPreparedStatement}.
 *
 * <p>Parameter markers ({@code ?}) outside of comments, single-quoted string
 * literals, and double-quoted identifiers are replaced positionally with the
 * literal form of the bound value.</p>
 */
final class SqlParameterSubstitutor {

    /** Rough size estimate of one inlined literal, used for buffer sizing. */
    private static final int LITERAL_SIZE_ESTIMATE = 10;

    private SqlParameterSubstitutor() {}

    /**
     * Substitutes parameter markers with their bound literal values.
     * Returns the final SQL string ready for execution.
     *
     * @throws SQLException if a marker has no bound parameter
     */
    static String substitute(String sql, Map<Integer, Object> parameters) throws SQLException {
        if (parameters.isEmpty() && !sql.contains("?")) {
            return sql;
        }
        SqlTextScanner scanner = new SqlTextScanner(sql);
        StringBuilder result = new StringBuilder(sql.length() + parameters.size() * LITERAL_SIZE_ESTIMATE);
        int paramIndex = 1;
        while (scanner.hasMoreText()) {
            result.append(scanner.nextSegment());
            if (scanner.foundParameterMarker()) {
                if (!parameters.containsKey(paramIndex)) {
                    throw new SQLException("Parameter " + paramIndex + " not set");
                }
                result.append(toLiteral(parameters.get(paramIndex)));
                paramIndex++;
            }
        }
        return result.toString();
    }

    /**
     * Counts the number of parameter placeholders in a SQL string,
     * ignoring question marks inside comments, single-quoted string literals,
     * and double-quoted identifiers.
     */
    static int countPlaceholders(String sql) {
        SqlTextScanner scanner = new SqlTextScanner(sql);
        int count = 0;
        while (scanner.hasMoreText()) {
            scanner.nextSegment();
            if (scanner.foundParameterMarker()) {
                count++;
            }
        }
        return count;
    }

    private static String toLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        String typed = toTypedLiteral(value);
        if (typed != null) {
            return typed;
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }

    private static String toTypedLiteral(Object value) {
        if (value instanceof Boolean b) {
            return b ? "TRUE" : "FALSE";
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        return toTemporalLiteral(value);
    }

    private static String toTemporalLiteral(Object value) {
        if (value instanceof Date d) {
            return "DATE '" + d + "'";
        }
        if (value instanceof Timestamp ts) {
            return "TIMESTAMP '" + ts + "'";
        }
        if (value instanceof Time t) {
            return "TIME '" + t + "'";
        }
        return null;
    }

    /**
     * Walks a SQL string segment by segment, where each segment ends at an
     * unquoted parameter marker ({@code ?}) or at the end of the text.
     * Comments are copied into segments verbatim; quote state is tracked so
     * markers inside literals and identifiers are ignored.
     */
    private static final class SqlTextScanner {

        private final String sql;
        private int index;
        private boolean inSingleQuote;
        private boolean inDoubleQuote;
        private boolean markerFound;

        SqlTextScanner(String sql) {
            this.sql = sql;
        }

        boolean hasMoreText() {
            return index < sql.length();
        }

        boolean foundParameterMarker() {
            return markerFound;
        }

        /** Reads text up to (and consuming) the next unquoted '?' marker. */
        String nextSegment() {
            StringBuilder segment = new StringBuilder();
            markerFound = false;
            while (index < sql.length()) {
                if (appendCommentIfPresent(segment)) {
                    continue;
                }
                char c = sql.charAt(index);
                if (c == '\'' && !inDoubleQuote) {
                    appendSingleQuoteToken(segment);
                } else if (c == '"' && !inSingleQuote) {
                    inDoubleQuote = !inDoubleQuote;
                    segment.append(c);
                    index++;
                } else if (isParameterMarker(c)) {
                    index++;
                    markerFound = true;
                    return segment.toString();
                } else {
                    segment.append(c);
                    index++;
                }
            }
            return segment.toString();
        }

        private boolean isParameterMarker(char c) {
            return c == '?' && !inSingleQuote && !inDoubleQuote;
        }

        private boolean appendCommentIfPresent(StringBuilder segment) {
            boolean outsideQuotes = !inSingleQuote && !inDoubleQuote;
            char c = sql.charAt(index);
            if (outsideQuotes && c == '-' && nextCharIs('-')) {
                appendLineComment(segment);
                return true;
            }
            if (outsideQuotes && c == '/' && nextCharIs('*')) {
                appendBlockComment(segment);
                return true;
            }
            return false;
        }

        private boolean nextCharIs(char expected) {
            return index + 1 < sql.length() && sql.charAt(index + 1) == expected;
        }

        /** Appends a single-quote char, treating '' inside a literal as an escape. */
        private void appendSingleQuoteToken(StringBuilder segment) {
            if (inSingleQuote && nextCharIs('\'')) {
                segment.append("''");
                index += 2;
                return;
            }
            inSingleQuote = !inSingleQuote;
            segment.append('\'');
            index++;
        }

        private void appendLineComment(StringBuilder segment) {
            int newline = sql.indexOf('\n', index);
            if (newline < 0) {
                segment.append(sql, index, sql.length());
                index = sql.length();
                return;
            }
            segment.append(sql, index, newline + 1);
            index = newline + 1;
        }

        private void appendBlockComment(StringBuilder segment) {
            int end = sql.indexOf("*/", index + 2);
            if (end < 0) {
                segment.append(sql, index, sql.length());
                index = sql.length();
                return;
            }
            segment.append(sql, index, end + 2);
            index = end + 2;
        }
    }
}
