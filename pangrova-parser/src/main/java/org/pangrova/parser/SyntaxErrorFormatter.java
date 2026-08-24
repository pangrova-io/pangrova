package org.pangrova.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.IntervalSet;

/**
 * Rewrites raw ANTLR syntax errors into Trino-style diagnostics that carry:
 * <ol>
 *   <li>an <b>expected-token hint</b> derived from the {@link RecognitionException}'s
 *       expected-token set (or, when unavailable, the recognizer's current
 *       expected set), mapped through the grammar {@link Vocabulary};</li>
 *   <li>a <b>caret-annotated source snippet</b> showing the offending line with a
 *       {@code ^} under the offending column; and</li>
 *   <li>a <b>stable error class / SQLSTATE classification</b> ({@code 42601}, the
 *       SQL standard class for <em>syntax error or access rule violation</em>,
 *       alongside the reused {@link PangrovaErrorCode#PARSER_SYNTAX_ERROR} code).</li>
 * </ol>
 *
 * <p>Shared by {@link ThrowingErrorListener} (strict parse) and
 * {@link CollectingErrorListener} (error-recovery parse) so both syntax-error
 * entry points emit identical, structured diagnostics.</p>
 */
final class SyntaxErrorFormatter {

    /**
     * SQLSTATE class for "syntax error or access rule violation" (SQL standard
     * class {@code 42}). Defined here, within the parser, because the shared
     * {@link PangrovaErrorCode} enum (in pangrova-common) does not carry SQLSTATEs;
     * the stable cross-module error code remains {@code PARSER-0001}.
     */
    static final String SQLSTATE_SYNTAX_ERROR = "42601";

    /** Maximum number of expected-token names rendered before truncating. */
    private static final int MAX_EXPECTED = 10;

    private SyntaxErrorFormatter() {
    }

    /**
     * Builds the enriched, multi-line diagnostic message.
     *
     * @param recognizer        the lexer or parser that raised the error
     * @param offendingSymbol   the offending token (may be {@code null} for lexer errors)
     * @param line              1-based line of the error
     * @param charPositionInLine 0-based column of the error
     * @param msg               the raw ANTLR message
     * @param e                 the recognition exception (may be {@code null})
     * @return a Trino-style diagnostic string
     */
    static String format(Recognizer<?, ?> recognizer, Object offendingSymbol,
                         int line, int charPositionInLine, String msg, RecognitionException e) {
        String base = stripExpectingTail(sanitizeInternalNames(msg));
        List<String> expected = expectedTokens(recognizer, e);

        StringBuilder sb = new StringBuilder();
        sb.append('[').append(PangrovaErrorCode.PARSER_SYNTAX_ERROR.getCode())
                .append("] [SQLSTATE ").append(SQLSTATE_SYNTAX_ERROR).append("] ")
                .append("line ").append(line).append(':').append(charPositionInLine)
                .append(' ').append(base);
        if (!expected.isEmpty()) {
            sb.append("; expecting one of: ").append(String.join(", ", expected));
        }

        String snippet = caretSnippet(sourceText(recognizer, offendingSymbol), line, charPositionInLine);
        if (!snippet.isEmpty()) {
            sb.append('\n').append(snippet);
        }
        return sb.toString();
    }

    /**
     * Sanitize ANTLR internal token names (e.g. {@code INTEGER_LITERAL},
     * {@code QUOTED_IDENTIFIER}) that leak into the base message. Only replaces
     * tokens containing an underscore, which are grammar-internal names; preserves
     * user-visible keywords like {@code SELECT}/{@code FROM}/{@code DROP}.
     */
    private static String sanitizeInternalNames(String msg) {
        if (msg == null) {
            return "";
        }
        return msg.replaceAll("'[A-Z][A-Z_]*_[A-Z_]*'", "<token>");
    }

    /**
     * Strip ANTLR's own raw {@code expecting {...}} tail so we can append our own
     * vocabulary-humanized expected-token hint without duplication.
     */
    private static String stripExpectingTail(String msg) {
        int idx = msg.indexOf(" expecting ");
        return idx >= 0 ? msg.substring(0, idx) : msg;
    }

    private static List<String> expectedTokens(Recognizer<?, ?> recognizer, RecognitionException e) {
        IntervalSet set = null;
        if (e != null) {
            try {
                set = e.getExpectedTokens();
            } catch (RuntimeException ignore) {
                // Some exception subtypes cannot compute expected tokens; fall through.
            }
        }
        if ((set == null || set.isNil()) && recognizer instanceof Parser parser) {
            try {
                set = parser.getExpectedTokens();
            } catch (RuntimeException ignore) {
                // Best-effort only.
            }
        }
        if (set == null || set.isNil()) {
            return List.of();
        }
        Vocabulary vocab = recognizer.getVocabulary();
        TreeSet<String> names = new TreeSet<>();
        for (int type : set.toList()) {
            names.add(displayName(vocab, type));
        }
        List<String> out = new ArrayList<>();
        int remaining = names.size();
        for (String n : names) {
            if (out.size() == MAX_EXPECTED) {
                out.add("(and " + remaining + " more)");
                break;
            }
            out.add(n);
            remaining--;
        }
        return out;
    }

    /**
     * Map a token type to a human-readable name: keyword/operator literals are
     * kept verbatim ({@code 'SELECT'}); symbolic names are humanized
     * ({@code INTEGER_LITERAL} -&gt; {@code <integer literal>}).
     */
    private static String displayName(Vocabulary vocab, int type) {
        if (type == Token.EOF) {
            return "<EOF>";
        }
        String literal = vocab.getLiteralName(type);
        if (literal != null) {
            return literal;
        }
        String symbolic = vocab.getSymbolicName(type);
        if (symbolic != null) {
            return "<" + symbolic.toLowerCase().replace('_', ' ') + ">";
        }
        return "<token " + type + ">";
    }

    /**
     * Render the offending source line plus a caret ({@code ^}) line aligned
     * under the offending column. Returns empty if the source text is unavailable.
     */
    private static String caretSnippet(String fullText, int line, int col) {
        if (fullText == null || fullText.isEmpty()) {
            return "";
        }
        String[] lines = fullText.split("\n", -1);
        if (line < 1 || line > lines.length) {
            return "";
        }
        String lineText = lines[line - 1].replace("\r", "");
        StringBuilder sb = new StringBuilder();
        sb.append("  ").append(lineText).append('\n').append("  ");
        int caret = Math.max(0, Math.min(col, lineText.length()));
        for (int i = 0; i < caret; i++) {
            // Preserve tab stops so the caret stays aligned in tab-indented SQL.
            sb.append(i < lineText.length() && lineText.charAt(i) == '\t' ? '\t' : ' ');
        }
        sb.append('^');
        return sb.toString();
    }

    /**
     * Recover the full source text from the recognizer or the offending token's
     * input stream so we can extract the offending line for the caret snippet.
     */
    private static String sourceText(Recognizer<?, ?> recognizer, Object offendingSymbol) {
        CharStream cs = null;
        if (offendingSymbol instanceof Token token) {
            cs = token.getInputStream();
        }
        if (cs == null && recognizer != null) {
            IntStream input = recognizer.getInputStream();
            if (input instanceof CharStream charStream) {
                cs = charStream;
            } else if (input instanceof TokenStream tokenStream
                    && tokenStream.getTokenSource() != null) {
                cs = tokenStream.getTokenSource().getInputStream();
            }
        }
        if (cs == null) {
            return "";
        }
        try {
            return cs.getText(Interval.of(0, cs.size() - 1));
        } catch (RuntimeException ignore) {
            return "";
        }
    }
}
