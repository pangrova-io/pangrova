package org.pangrova.parser.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.SourceLocation;

/**
 * Parse error carrying the exact source position (line and column).
 */
public class ParseException extends PangrovaException {

    private final int line;
    private final int column;
    private final SourceLocation sourceLocation;

    /**
     * Constructs a parse exception with a line and column.
     *
     * @param errorCode structured error code
     * @param message human-readable error description
     * @param line one-based source line
     * @param column zero-based source column
     */
    public ParseException(PangrovaErrorCode errorCode, String message, int line, int column) {
        super(errorCode, message);
        this.line = line;
        this.column = column;
        this.sourceLocation = null;
    }

    /**
     * Constructs a ParseException with full SourceLocation and a root cause.
     *
     * @param errorCode      structured error code
     * @param message        human-readable description including the invalid text
     * @param sourceLocation precise source position of the offending token
     * @param cause          the underlying exception (e.g. NumberFormatException)
     */
    public ParseException(PangrovaErrorCode errorCode, String message,
                          SourceLocation sourceLocation, Throwable cause) {
        super(errorCode, message, cause);
        this.sourceLocation = sourceLocation;
        this.line = sourceLocation != null ? sourceLocation.line() : 0;
        this.column = sourceLocation != null ? sourceLocation.col() : 0;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    /**
     * Returns the full source location if available, or null for legacy call sites.
     */
    public SourceLocation getSourceLocation() {
        return sourceLocation;
    }
}
