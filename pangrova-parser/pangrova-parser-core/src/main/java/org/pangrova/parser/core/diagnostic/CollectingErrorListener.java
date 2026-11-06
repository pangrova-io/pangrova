package org.pangrova.parser.core.diagnostic;

import org.pangrova.parser.api.diagnostic.ParseError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

/**
 * ANTLR error listener that collects syntax errors as {@link ParseError} instances
 * instead of throwing exceptions. Used in error-recovery parsing mode to accumulate
 * all errors while allowing the parser to continue producing a partial parse tree.
 * Uses {@link SyntaxErrorFormatter} to produce enriched diagnostics.
 */
public final class CollectingErrorListener extends BaseErrorListener {

    private final List<ParseError> errors = new ArrayList<>();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        String tokenText = null;
        if (offendingSymbol instanceof Token token) {
            tokenText = token.getText();
        }
        String formatted = SyntaxErrorFormatter.format(
                recognizer, offendingSymbol, line, charPositionInLine, msg, e);
        errors.add(new ParseError(line, charPositionInLine, formatted, tokenText));
    }

    /**
     * Returns an unmodifiable view of the collected errors.
     */
    public List<ParseError> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    /**
     * Returns true if any syntax errors were collected.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
