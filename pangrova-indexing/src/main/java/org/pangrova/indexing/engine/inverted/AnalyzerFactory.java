package org.pangrova.indexing.engine.inverted;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Factory for text analyzers that tokenize strings for inverted index construction.
 */
final class AnalyzerFactory {

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern NON_ALPHA = Pattern.compile("[^\\p{L}\\p{N}]+");

    private AnalyzerFactory() {}

    /**
     * Tokenizes the input string using the specified analyzer.
     *
     * @param text     input text
     * @param analyzer analyzer name ("standard" or "cjk")
     * @return list of tokens
     */
    static List<String> tokenize(String text, String analyzer) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        return switch (analyzer.toLowerCase(Locale.ROOT)) {
            case "cjk" -> tokenizeCjk(text);
            default -> tokenizeStandard(text);
        };
    }

    private static List<String> tokenizeStandard(String text) {
        String cleaned = NON_ALPHA.matcher(text).replaceAll(" ").trim();
        if (cleaned.isEmpty()) return List.of();
        String[] parts = WHITESPACE.split(cleaned.toLowerCase(Locale.ROOT));
        List<String> tokens = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (!part.isEmpty()) {
                tokens.add(part);
            }
        }
        return tokens;
    }

    private static List<String> tokenizeCjk(String text) {
        List<String> tokens = new ArrayList<>();
        StringBuilder nonCjk = new StringBuilder();
        for (int i = 0; i < text.length(); ) {
            int cp = text.codePointAt(i);
            if (isCjk(cp)) {
                flushNonCjk(nonCjk, tokens);
                emitCjkBigram(text, i, cp, tokens);
            } else {
                nonCjk.appendCodePoint(cp);
            }
            i += Character.charCount(cp);
        }
        flushNonCjk(nonCjk, tokens);
        return tokens;
    }

    private static void flushNonCjk(StringBuilder nonCjk, List<String> tokens) {
        if (!nonCjk.isEmpty()) {
            tokens.addAll(tokenizeStandard(nonCjk.toString()));
            nonCjk.setLength(0);
        }
    }

    private static void emitCjkBigram(String text, int i, int cp, List<String> tokens) {
        String current = new String(Character.toChars(cp));
        int nextIdx = i + Character.charCount(cp);
        if (nextIdx < text.length() && isCjk(text.codePointAt(nextIdx))) {
            tokens.add(current + new String(Character.toChars(text.codePointAt(nextIdx))));
        }
        tokens.add(current);
    }

    private static boolean isCjk(int codePoint) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
        return block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || block == Character.UnicodeBlock.HIRAGANA
                || block == Character.UnicodeBlock.KATAKANA
                || block == Character.UnicodeBlock.HANGUL_SYLLABLES;
    }
}
