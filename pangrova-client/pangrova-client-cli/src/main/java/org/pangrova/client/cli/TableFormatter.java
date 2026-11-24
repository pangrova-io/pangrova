package org.pangrova.client.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Formats query results as an aligned ASCII table.
 *
 * <p>Example output:
 * <pre>
 * +------+--------+
 * | id   | name   |
 * +------+--------+
 * | 1    | Alice  |
 * | 2    | Bob    |
 * +------+--------+
 * </pre>
 */
final class TableFormatter {
    private static final int LINE_SEPARATOR = 0x2028;
    private static final int PARAGRAPH_SEPARATOR = 0x2029;

    private TableFormatter() {}

    static String formatPage(List<String> columnNames, List<List<String>> rows) {
        if (columnNames.isEmpty()) return "(no columns)\n";

        int columnCount = columnNames.size();
        List<Integer> widths = computeColumnWidths(columnNames, rows);

        StringBuilder sb = new StringBuilder();
        String separator = buildSeparator(widths);

        sb.append(separator);
        sb.append(buildRow(columnNames, widths));
        sb.append(separator);

        for (List<String> row : rows) {
            List<String> paddedRow = padRow(row, columnCount);
            sb.append(buildRow(paddedRow, widths));
        }

        sb.append(separator);
        return sb.toString();
    }

    static final int MAX_CELL_WIDTH = 64;

    static String formatCell(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof float[] vector) {
            return truncate(Arrays.toString(vector));
        }
        if (value instanceof double[] vector) {
            return truncate(Arrays.toString(vector));
        }
        if (value instanceof Float[] vector) {
            return truncate(Arrays.toString(vector));
        }
        if (value instanceof Double[] vector) {
            return truncate(Arrays.toString(vector));
        }
        return truncate(value.toString());
    }

    private static String truncate(String value) {
        if (value.length() <= MAX_CELL_WIDTH) {
            return value;
        }
        return value.substring(0, MAX_CELL_WIDTH - 3) + "...";
    }

    private static List<Integer> computeColumnWidths(List<String> headers, List<List<String>> rows) {
        List<Integer> widths = new ArrayList<>(headers.size());
        for (String header : headers) {
            widths.add(displayLength(header));
        }
        for (List<String> row : rows) {
            for (int i = 0; i < headers.size() && i < row.size(); i++) {
                int len = displayLength(row.get(i));
                if (len > widths.get(i)) {
                    widths.set(i, len);
                }
            }
        }
        return widths;
    }

    private static String buildSeparator(List<Integer> widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) {
            sb.append("-".repeat(w + 2)).append("+");
        }
        sb.append('\n');
        return sb.toString();
    }

    private static String buildRow(List<String> values, List<Integer> widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.size(); i++) {
            String val = i < values.size() ? displayValue(values.get(i)) : "";
            sb.append(' ').append(padRight(val, widths.get(i))).append(" |");
        }
        sb.append('\n');
        return sb.toString();
    }

    private static List<String> padRow(List<String> row, int expectedSize) {
        if (row.size() >= expectedSize) {
            return row;
        }
        List<String> padded = new ArrayList<>(row);
        while (padded.size() < expectedSize) {
            padded.add("");
        }
        return padded;
    }

    private static String padRight(String value, int width) {
        int padding = width - displayWidth(value);
        if (padding <= 0) {
            return value;
        }
        return value + " ".repeat(padding);
    }

    private static int displayLength(String value) {
        return displayWidth(displayValue(value));
    }

    /**
     * Computes the terminal display width of a string, accounting for
     * East Asian Wide and Fullwidth characters (CJK) that occupy 2 columns.
     * Based on Unicode East Asian Width property (UAX #11).
     */
    static int displayWidth(String value) {
        int width = 0;
        for (int i = 0; i < value.length(); ) {
            int cp = value.codePointAt(i);
            width += isEastAsianWide(cp) ? 2 : 1;
            i += Character.charCount(cp);
        }
        return width;
    }

    /** Unicode East Asian Wide/Fullwidth ranges (UAX #11). Each pair is [start, end] inclusive. */
    private static final int[][] WIDE_RANGES = {
            {0x2E80, 0x303E},   // CJK Radicals Supplement .. Ideographic Description Characters
            {0x3041, 0x33BF},   // Hiragana, Katakana, Bopomofo, Hangul Compat Jamo, Kanbun, CJK Compat
            {0x3400, 0x4DBF},   // CJK Unified Ideographs Extension A
            {0x4E00, 0x9FFF},   // CJK Unified Ideographs
            {0xA000, 0xA4CF},   // Yi Syllables and Yi Radicals
            {0xAC00, 0xD7AF},   // Hangul Syllables
            {0xF900, 0xFAFF},   // CJK Compatibility Ideographs
            {0xFF01, 0xFF60},   // Fullwidth Forms (Fullwidth ASCII variants)
            {0xFFE0, 0xFFE6},   // Fullwidth Signs
            {0x20000, 0x2FA1F}, // CJK Unified Ideographs Extension B and beyond (SMP)
            {0x30000, 0x323AF}, // CJK Unified Ideographs Extension G/H/I
    };

    /**
     * Returns true if the code point is East Asian Wide or Fullwidth (W/F),
     * meaning it occupies 2 terminal columns. Covers CJK Unified Ideographs,
     * CJK Compatibility Ideographs, Hangul Syllables, Fullwidth Forms, and
     * other commonly double-width ranges per UAX #11.
     */
    private static boolean isEastAsianWide(int cp) {
        for (int[] range : WIDE_RANGES) {
            if (cp >= range[0] && cp <= range[1]) return true;
        }
        return false;
    }

    private static String displayValue(String value) {
        if (value == null) {
            return "NULL";
        }
        StringBuilder escaped = null;
        for (int offset = 0; offset < value.length(); ) {
            int codePoint = value.codePointAt(offset);
            String replacement = escapeCodePoint(codePoint);
            if (replacement != null) {
                if (escaped == null) {
                    escaped = new StringBuilder(value.length()).append(value, 0, offset);
                }
                escaped.append(replacement);
            } else if (escaped != null) {
                escaped.appendCodePoint(codePoint);
            }
            offset += Character.charCount(codePoint);
        }
        return escaped == null ? value : escaped.toString();
    }

    private static String escapeCodePoint(int codePoint) {
        return switch (codePoint) {
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            case '\t' -> "\\t";
            case '\\' -> "\\\\";
            case '|' -> "\\u007c";
            default -> escapeControl(codePoint);
        };
    }

    private static String escapeControl(int codePoint) {
        if (!Character.isISOControl(codePoint)
                && codePoint != LINE_SEPARATOR && codePoint != PARAGRAPH_SEPARATOR) {
            return null;
        }
        String hex = Integer.toHexString(codePoint);
        return "\\u" + "0000".substring(hex.length()) + hex;
    }
}
