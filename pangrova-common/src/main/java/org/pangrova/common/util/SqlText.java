package org.pangrova.common.util;

/** SQL text keys shared by the parse cache and diagnostic plan baselines. */
public final class SqlText {
    private SqlText() {}

    /** Preserves identifier case and all text within quoted tokens and comments. */
    public static String parseCacheKey(String sql) {
        return normalize(sql, false);
    }

    /** Folds unquoted ASCII letters when comparing diagnostic plan baselines. */
    public static String baselineKey(String sql) {
        return normalize(sql, true);
    }

    private static String normalize(String sql, boolean foldCase) {
        StringBuilder key = new StringBuilder(sql.length());
        boolean pendingSpace = false;
        for (int i = 0; i < sql.length();) {
            char c = sql.charAt(i);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                pendingSpace = !key.isEmpty();
                i++;
                continue;
            }
            if (pendingSpace) {
                key.append(' ');
                pendingSpace = false;
            }
            if (c == '\'' || c == '"' || c == '`') {
                int end = quotedEnd(sql, i, c);
                key.append(sql, i, end);
                i = end;
            } else if (sql.startsWith("--", i)) {
                int end = i + 2;
                while (end < sql.length() && sql.charAt(end) != '\r' && sql.charAt(end) != '\n') {
                    end++;
                }
                key.append(sql, i, end);
                if (end < sql.length()) {
                    key.append('\n');
                    end++;
                    if (sql.charAt(end - 1) == '\r' && end < sql.length() && sql.charAt(end) == '\n') {
                        end++;
                    }
                }
                i = end;
            } else if (sql.startsWith("/*", i)) {
                int close = sql.indexOf("*/", i + 2);
                int end = close < 0 ? sql.length() : close + 2;
                key.append(sql, i, end);
                i = end;
            } else {
                key.append(foldCase && c >= 'A' && c <= 'Z' ? Character.toLowerCase(c) : c);
                i++;
            }
        }
        return key.toString();
    }

    private static int quotedEnd(String sql, int start, char delimiter) {
        for (int i = start + 1; i < sql.length(); i++) {
            if (sql.charAt(i) == delimiter) {
                if (i + 1 == sql.length() || sql.charAt(i + 1) != delimiter) {
                    return i + 1;
                }
                i++;
            }
        }
        return sql.length();
    }
}
