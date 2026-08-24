package org.pangrova.parser;

/**
 * Represents a single syntax error encountered during error-recovery parsing.
 * Carries the source position and a human-readable message.
 *
 * @param line            1-based line number where the error occurred
 * @param column          0-based column (character position) within the line
 * @param message         human-readable description of the syntax error
 * @param offendingToken  the token text that triggered the error, or null if unavailable
 */
public record ParseError(int line, int column, String message, String offendingToken) {

    /**
     * Compact constructor with validation.
     */
    public ParseError {
        if (line < 0) {
            throw new IllegalArgumentException("line must be non-negative");
        }
        if (column < 0) {
            throw new IllegalArgumentException("column must be non-negative");
        }
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be null or blank");
        }
    }

    /**
     * Creates a ParseError without an offending token.
     */
    public ParseError(int line, int column, String message) {
        this(line, column, message, null);
    }

    @Override
    public String toString() {
        String location = "line " + line + ":" + column;
        if (offendingToken != null) {
            return location + " at '" + offendingToken + "': " + message;
        }
        return location + ": " + message;
    }
}
