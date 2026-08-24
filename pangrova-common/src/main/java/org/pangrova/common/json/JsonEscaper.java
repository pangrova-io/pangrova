package org.pangrova.common.json;

/**
 * Escapes string values for safe embedding in hand-built JSON per RFC 8259 §7.
 *
 * <p>Handles mandatory escapes ({@code "}, {@code \}, control characters U+0000–U+001F)
 * and produces valid JSON string content without surrounding quotes.
 *
 * <p>This utility has zero external dependencies and is safe for use in any module.
 */
public final class JsonEscaper {

    private JsonEscaper() {}

    /**
     * Escapes a string value for embedding inside a JSON quoted string.
     *
     * <p>The returned value does not include surrounding double quotes.
     * Callers are responsible for wrapping the result in quotes when building JSON.
     *
     * @param value the raw string to escape; must not be null
     * @return the escaped string safe for JSON embedding
     * @throws NullPointerException if value is null
     */
    public static String escape(String value) {
        if (value == null) {
            throw new NullPointerException("value must not be null");
        }
        return escapeChars(value);
    }

    private static String escapeChars(String value) {
        StringBuilder sb = null;
        int start = 0;
        int length = value.length();

        for (int i = 0; i < length; i++) {
            String replacement = replacementFor(value.charAt(i));
            if (replacement != null) {
                if (sb == null) {
                    sb = new StringBuilder(length + 16);
                }
                sb.append(value, start, i).append(replacement);
                start = i + 1;
            }
        }

        if (sb == null) {
            return value;
        }
        return sb.append(value, start, length).toString();
    }

    private static String replacementFor(char ch) {
        return switch (ch) {
            case '"' -> "\\\"";
            case '\\' -> "\\\\";
            case '\b' -> "\\b";
            case '\f' -> "\\f";
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            case '\t' -> "\\t";
            default -> {
                if (ch < 0x20) {
                    yield String.format("\\u%04x", (int) ch);
                }
                yield null;
            }
        };
    }
}
