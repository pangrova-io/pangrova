package org.pangrova.client.cli;

/**
 * ANSI presentation shared by the interactive shell and result tables.
 * Color is only enabled on an attached console and honors {@code NO_COLOR}.
 */
final class TerminalStyle {

    private static final String ESC = "\u001B[";
    private static final String RESET = ESC + "0m";
    private static final String ACCENT = ESC + "38;5;141m";
    private static final String HEADER = ESC + "1;38;5;141m";
    private static final String PROMPT = ESC + "1;38;5;141m";
    private static final String BOLD = ESC + "1m";
    private static final String DIM = ESC + "2m";
    private static final String SUCCESS = ESC + "38;5;114m";
    private static final String ERROR = ESC + "1;38;5;203m";
    private static final int DEFAULT_WIDTH = 100;
    private static final int MIN_WIDTH = 40;
    private static final int MAX_WIDTH = 240;

    private final boolean color;
    private final int width;

    TerminalStyle(boolean color, int width) {
        this.color = color;
        this.width = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, width));
    }

    static TerminalStyle plain() {
        return new TerminalStyle(false, DEFAULT_WIDTH);
    }

    static TerminalStyle detect() {
        String noColor = System.getenv("NO_COLOR");
        String term = System.getenv("TERM");
        boolean attached = System.console() != null;
        boolean enabled = attached && noColor == null && !"dumb".equalsIgnoreCase(term);
        return new TerminalStyle(enabled, parseWidth(System.getenv("COLUMNS")));
    }

    int width() {
        return width;
    }

    String accent(String text) {
        return wrap(ACCENT, text);
    }

    String header(String text) {
        return wrap(HEADER, text);
    }

    String prompt(String text) {
        return wrap(PROMPT, text);
    }

    String bold(String text) {
        return wrap(BOLD, text);
    }

    String dim(String text) {
        return wrap(DIM, text);
    }

    String success(String text) {
        return wrap(SUCCESS, text);
    }

    String error(String text) {
        return wrap(ERROR, text);
    }

    private String wrap(String prefix, String text) {
        return color ? prefix + text + RESET : text;
    }

    private static int parseWidth(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_WIDTH;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ignored) {
            return DEFAULT_WIDTH;
        }
    }
}
