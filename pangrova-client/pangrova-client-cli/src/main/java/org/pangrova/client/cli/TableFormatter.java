package org.pangrova.client.cli;

import java.util.ArrayList;
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
 * 2 rows
 * </pre>
 */
final class TableFormatter {

    private TableFormatter() {}

    /**
     * Formats column names and data rows into an ASCII table string.
     *
     * @param columnNames column header names
     * @param rows        data rows; each row must have the same size as columnNames
     * @return formatted ASCII table
     */
    static String format(List<String> columnNames, List<List<String>> rows) {
        if (columnNames.isEmpty()) {
            return "(no columns)\n";
        }

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
        sb.append(rows.size()).append(rows.size() == 1 ? " row" : " rows").append('\n');
        return sb.toString();
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
        return value == null ? 4 : displayWidth(value); // "NULL" is 4 chars
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
        return value == null ? "NULL" : value;
    }
}
