package org.pangrova.parser.diagnostic;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * ANTLR4 error listener that converts syntax errors into {@link ParseException}.
 * Uses {@link SyntaxErrorFormatter} to produce enriched diagnostics with
 * expected-token hints, caret-annotated source snippets, and SQLSTATE codes.
 */
public class ThrowingErrorListener extends BaseErrorListener {

    public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        String formatted = SyntaxErrorFormatter.format(
                recognizer, offendingSymbol, line, charPositionInLine, msg, e);
        throw new ParseException(
                PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                formatted,
                line,
                charPositionInLine);
    }
}
